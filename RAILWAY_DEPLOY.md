# Deploying to Railway

This project is configured to run on [Railway](https://railway.app/) as a background worker that executes the trading cycle every 4 hours.

## Prerequisites

1.  A Railway account.
2.  A GitHub repository containing this code.

## Setup Instructions

1.  **New Project**:
    - Go to Railway Dashboard.
    - Click "New Project" -> "Deploy from GitHub repo".
    - Select your repository.

2.  **Environment Variables**:
    - Go to the "Variables" tab in your Railway project.
    - Add the following variables:

    | Variable | Description |
    | :--- | :--- |
    | `DEEPSEEK_API_KEY` | Your DeepSeek API Key. |
    | `ETHERSCAN_API_KEY`| Your Etherscan API Key. |
    | `GITHUB_TOKEN` | A GitHub Personal Access Token (Classic) with `repo` scope. This is needed to push data back to the repo. |
    | `REPO_URL` | The full HTTPS URL of your repo (e.g., `https://github.com/yourusername/your-repo.git`). |
    | `USE_LOCAL_PROXY` | Set to `0` (False). |
    | `USE_LOCAL_QLIB` | Set to `0` (False). |

3.  **Deploy**:
    - Railway should automatically detect the `Dockerfile` and start building.
    - Once deployed, the service will start `scheduler.py`.
    - It will run the trading cycle immediately upon start, and then every 4 hours.

## Troubleshooting

- **"You must specify a workspaceId"**: If you see this error in the CLI, make sure you are logged in (`railway login`) and have created a project (`railway init` or link via UI).
- **Git Push Fails**: Check the `GITHUB_TOKEN` permissions. It must have "Write" access to the repository contents (or full `repo` scope for classic tokens).
- **Logs**: Check the "Deployments" -> "View Logs" tab in Railway to see the output of the trading bot.
