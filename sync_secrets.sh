gh secret list | awk '{print $1}' | xargs -I {} gh secret delete {}
gh secret set -f .env
