# Push this repo to GitHub
# 1. Create a new EMPTY repo at https://github.com/new (e.g. name: BACKENDBLOCKID)
# 2. Set YOUR_USERNAME and YOUR_REPO below, then run: .\push-to-github.ps1

$GITHUB_USER = "YOUR_USERNAME"   # e.g. johndoe
$REPO_NAME   = "YOUR_REPO"      # e.g. BACKENDBLOCKID

$remote = "https://github.com/$GITHUB_USER/$REPO_NAME.git"
git remote add origin $remote 2>$null; git remote set-url origin $remote
git branch -M main
git push -u origin main
