import schedule
import time
import subprocess
import os
import shutil
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_trading_cycle():
    logger.info("Starting trading cycle...")
    try:
        # 1. Run the main trading script
        # Using subprocess to run it as a separate process, similar to how it runs in shell
        result = subprocess.run(["python", "run_daily_cycle.py"], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("Trading cycle completed successfully.")
            logger.info(result.stdout)
        else:
            logger.error("Trading cycle failed.")
            logger.error(result.stderr)
            # We might still want to commit logs if they exist, but for now let's proceed
            
        # 2. Sync files to frontend directory (matching GitHub Actions logic)
        logger.info("Syncing files to frontend directory...")
        os.makedirs("frontpages/public/data", exist_ok=True)
        
        files_to_sync = [
            "portfolio_state.json",
            "trade_log.csv",
            "agent_decision_log.json"
        ]
        
        for filename in files_to_sync:
            if os.path.exists(filename):
                shutil.copy(filename, f"frontpages/public/data/{filename}")
                logger.info(f"Copied {filename}")
            else:
                logger.warning(f"File {filename} not found, skipping copy.")

        # 3. Commit and Push to GitHub
        push_to_github()
        
    except Exception as e:
        logger.exception(f"An error occurred during the trading cycle: {e}")

def push_to_github():
    logger.info("Preparing to push changes to GitHub...")
    
    github_token = os.environ.get("GITHUB_TOKEN")
    repo_url = os.environ.get("REPO_URL") # e.g., https://github.com/username/repo.git
    
    if not github_token or not repo_url:
        logger.warning("GITHUB_TOKEN or REPO_URL not set. Skipping git push.")
        return

    # Construct auth URL
    if "https://" in repo_url:
        auth_repo_url = repo_url.replace("https://", f"https://{github_token}@")
    else:
        logger.error("REPO_URL must start with https://")
        return

    # Use a temporary directory for git operations to avoid issues with the container's file system
    # and missing .git directory.
    temp_dir = "temp_git_repo"
    
    try:
        # Remove temp dir if it exists (cleanup from previous run if failed)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            
        # 1. Clone the repo
        logger.info("Cloning repository...")
        subprocess.run(["git", "clone", "--depth", "1", auth_repo_url, temp_dir], check=True)
        
        # 2. Configure git in the temp repo
        subprocess.run(["git", "config", "user.name", "Railway Bot"], cwd=temp_dir, check=True)
        subprocess.run(["git", "config", "user.email", "bot@railway.app"], cwd=temp_dir, check=True)
        
        # 3. Copy updated files TO the temp repo
        # We need to preserve the directory structure: frontpages/public/data/ and root files
        files_to_sync = [
            "portfolio_state.json",
            "trade_log.csv",
            "agent_decision_log.json"
        ]
        
        # Copy root files
        for f in files_to_sync:
            if os.path.exists(f):
                shutil.copy(f, os.path.join(temp_dir, f))
                
        # Copy frontend files
        frontend_data_dir = os.path.join(temp_dir, "frontpages/public/data")
        os.makedirs(frontend_data_dir, exist_ok=True)
        
        for f in files_to_sync:
            # Source is the root file we just generated
            if os.path.exists(f):
                shutil.copy(f, os.path.join(frontend_data_dir, f))

        # 4. Add, Commit, Push
        logger.info("Committing changes...")
        subprocess.run(["git", "add", "."], cwd=temp_dir, check=True)
        
        # Check for changes
        status = subprocess.run(["git", "status", "--porcelain"], cwd=temp_dir, capture_output=True, text=True)
        if not status.stdout.strip():
            logger.info("No changes to commit.")
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(["git", "commit", "-m", f"🤖 Auto-update trading data {timestamp} [skip ci]"], cwd=temp_dir, check=True)
        
        logger.info("Pushing to remote...")
        subprocess.run(["git", "push"], cwd=temp_dir, check=True)
        logger.info("Successfully pushed changes to GitHub.")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Git operation failed: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error during git push: {e}")
    finally:
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def main():
    logger.info("Scheduler started. Waiting for next cycle...")
    
    # Run once on startup to ensure data is fresh (optional)
    # run_trading_cycle()
    
    # Schedule at specific times (UTC) to align with 4H candle closes
    # 00:00, 04:00, 08:00, 12:00, 16:00, 20:00
    times = ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00"]
    
    for t in times:
        schedule.every().day.at(t).do(run_trading_cycle)
        logger.info(f"Scheduled run at {t} UTC")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
