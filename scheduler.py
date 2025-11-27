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
    # Format: https://<TOKEN>@github.com/<USERNAME>/<REPO>.git
    # Assuming REPO_URL is standard https
    if "https://" in repo_url:
        auth_repo_url = repo_url.replace("https://", f"https://{github_token}@")
    else:
        logger.error("REPO_URL must start with https://")
        return

    try:
        # Configure git
        subprocess.run(["git", "config", "--global", "user.name", "Railway Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@railway.app"], check=True)
        
        # Add files
        files_to_add = [
            "frontpages/public/data/portfolio_state.json",
            "frontpages/public/data/trade_log.csv",
            "frontpages/public/data/agent_decision_log.json",
            "portfolio_state.json",
            "trade_log.csv",
            "agent_decision_log.json"
        ]
        
        for f in files_to_add:
            if os.path.exists(f):
                subprocess.run(["git", "add", f], check=True)
        
        # Check if there are changes
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if not status.stdout.strip():
            logger.info("No changes to commit.")
            return

        # Commit
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        subprocess.run(["git", "commit", "-m", f"🤖 Auto-update trading data {timestamp} [skip ci]"], check=True)
        
        # Push
        logger.info("Pushing to remote...")
        subprocess.run(["git", "push", auth_repo_url, "HEAD:main"], check=True) # Assuming main branch
        logger.info("Successfully pushed changes to GitHub.")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Git operation failed: {e}")

def main():
    logger.info("Scheduler started. Waiting for next cycle...")
    
    # Run immediately on startup? 
    # Maybe better to wait for the schedule to avoid double runs on restart, 
    # but for a bot usually we want it to run once to verify.
    # Let's run once on startup to ensure data is fresh.
    run_trading_cycle()
    
    # Schedule every 4 hours
    schedule.every(4).hours.do(run_trading_cycle)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
