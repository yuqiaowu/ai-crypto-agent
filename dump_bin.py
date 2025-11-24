# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import shutil
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

class DumpData:
    def __init__(self, csv_path, qlib_dir, symbol_field_name="symbol", date_field_name="date", include_fields=None, exclude_fields=None):
        self.csv_path = csv_path
        self.qlib_dir = Path(qlib_dir)
        self.symbol_field_name = symbol_field_name
        self.date_field_name = date_field_name
        self.include_fields = include_fields
        self.exclude_fields = exclude_fields
        self.features_dir = self.qlib_dir / "features"
        self.calendars_dir = self.qlib_dir / "calendars"
        self.instruments_dir = self.qlib_dir / "instruments"

    def _get_all_columns(self, df):
        cols = list(df.columns)
        if self.include_fields:
            cols = [c for c in cols if c in self.include_fields]
        if self.exclude_fields:
            cols = [c for c in cols if c not in self.exclude_fields]
        
        # Ensure symbol and date are not in features
        if self.symbol_field_name in cols:
            cols.remove(self.symbol_field_name)
        if self.date_field_name in cols:
            cols.remove(self.date_field_name)
        return cols

    def dump(self):
        print(f"Loading CSV from {self.csv_path}...")
        df = pd.read_csv(self.csv_path)
        
        # Parse date
        df[self.date_field_name] = pd.to_datetime(df[self.date_field_name], errors="coerce")
        df[self.date_field_name] = df[self.date_field_name].dt.tz_localize(None)
        before = len(df)
        df = df.dropna(subset=[self.date_field_name])
        if len(df) != before:
            print(f"Dropped {before - len(df)} rows with invalid datetime during dump")
        
        # Sort
        df = df.sort_values([self.symbol_field_name, self.date_field_name])
        
        # Get all dates and instruments
        all_dates = sorted(df[self.date_field_name].unique())
        calendar_index = pd.DatetimeIndex(all_dates)
        instruments = sorted(df[self.symbol_field_name].unique())
        
        # Map date to index
        date_map = {date: i for i, date in enumerate(all_dates)}
        
        # Create directories
        self.features_dir.mkdir(parents=True, exist_ok=True)
        self.calendars_dir.mkdir(parents=True, exist_ok=True)
        self.instruments_dir.mkdir(parents=True, exist_ok=True)
        
        # Dump calendar
        print("Dumping calendar...")
        pd.Series(all_dates).to_csv(self.calendars_dir / "day.txt", index=False, header=False)
        
        # Dump instruments
        print("Dumping instruments...")
        # Format: symbol, start_date, end_date
        inst_data = []
        for inst in instruments:
            inst_df = df[df[self.symbol_field_name] == inst]
            start_date = inst_df[self.date_field_name].min()
            end_date = inst_df[self.date_field_name].max()
            inst_data.append([inst, start_date, end_date])
        
        pd.DataFrame(inst_data).to_csv(self.instruments_dir / "all.txt", sep="\t", index=False, header=False)
        
        # Dump features
        print("Dumping features...")
        feature_cols = self._get_all_columns(df)
        
        version_file = self.qlib_dir / ".bin_format_v2"
        for inst in tqdm(instruments):
            inst_df = df[df[self.symbol_field_name] == inst].set_index(self.date_field_name)
            inst_df = inst_df.sort_index()
            if inst_df.empty:
                continue
            start_time = inst_df.index.min()
            end_time = inst_df.index.max()
            inst_calendar = calendar_index[(calendar_index >= start_time) & (calendar_index <= end_time)]
            inst_df = inst_df.reindex(inst_calendar)
            start_idx = date_map[start_time]
            
            inst_dir = self.features_dir / inst.lower() # Qlib uses lowercase for instrument folders
            inst_dir.mkdir(parents=True, exist_ok=True)
            
            for col in feature_cols:
                # Convert to float32
                values = inst_df[col].astype(np.float32).values
                data = np.concatenate([[start_idx], values]).astype(np.float32)
                
                # Save as binary
                with open(inst_dir / f"{col.lower()}.day.bin", "wb") as f:
                    data.tofile(f)

        print("✅ Dump finished!")
        version_file.write_text("with_start_index\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv_path", type=str, required=True)
    parser.add_argument("--qlib_dir", type=str, required=True)
    parser.add_argument("--symbol_field_name", type=str, default="symbol")
    parser.add_argument("--date_field_name", type=str, default="date")
    parser.add_argument("--include_fields", type=str, default=None)
    
    args = parser.parse_args()
    
    include_fields = args.include_fields.split(",") if args.include_fields else None
    
    dumper = DumpData(
        csv_path=args.csv_path,
        qlib_dir=args.qlib_dir,
        symbol_field_name=args.symbol_field_name,
        date_field_name=args.date_field_name,
        include_fields=include_fields
    )
    dumper.dump()
