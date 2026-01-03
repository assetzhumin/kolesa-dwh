# GitHub Repository Setup Instructions

This guide will help you push your kolesa_dwh project to GitHub.

## Prerequisites

- GitHub account
- Git installed and configured
- GitHub CLI (`gh`) installed (optional, but recommended)

## Option 1: Using GitHub CLI (Recommended)

If you have GitHub CLI installed, this is the fastest method:

```bash
# Create a new public repository on GitHub
gh repo create kolesa_dwh --public --source=. --remote=origin --push

# Verify the remote was added
git remote -v
```

The `--push` flag will automatically push your initial commit to GitHub.

## Option 2: Using GitHub Web Interface

### Step 1: Create Repository on GitHub

1. Go to https://github.com/new
2. Repository name: `kolesa_dwh`
3. Description: "Dockerized data pipeline for kolesa.kz car listings using Airflow"
4. Visibility: **Public** (or Private if you prefer)
5. **DO NOT** initialize with README, .gitignore, or license (we already have these)
6. Click "Create repository"

### Step 2: Add Remote and Push

After creating the repository, GitHub will show you commands. Use these:

```bash
# Add the remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/kolesa_dwh.git

# Verify the remote
git remote -v

# Push to GitHub (use main branch)
git branch -M main
git push -u origin main
```

## Option 3: Using SSH (If you have SSH keys set up)

```bash
# Add SSH remote (replace YOUR_USERNAME with your GitHub username)
git remote add origin git@github.com:YOUR_USERNAME/kolesa_dwh.git

# Push to GitHub
git branch -M main
git push -u origin main
```

## Update README.md

After pushing, update the README.md file to replace `YOUR_USERNAME` with your actual GitHub username:

```bash
# Edit README.md and replace YOUR_USERNAME in the git clone URL
# Then commit and push:
git add README.md
git commit -m "Update README with actual GitHub repository URL"
git push
```

## Verify Setup

1. Visit your repository on GitHub: `https://github.com/YOUR_USERNAME/kolesa_dwh`
2. Verify all files are present
3. Verify `.env.example` is visible (but `.env` should NOT be visible)
4. Test cloning in a fresh directory:
   ```bash
   cd /tmp
   git clone https://github.com/YOUR_USERNAME/kolesa_dwh.git
   cd kolesa_dwh
   ls -la  # Verify .env.example exists but .env does not
   ```

## Optional: Branch Protection

For the main branch, consider enabling branch protection:

1. Go to your repository → Settings → Branches
2. Add rule for `main` branch
3. Enable:
   - Require pull request reviews before merging
   - Require status checks to pass before merging
   - Require branches to be up to date before merging

## Troubleshooting

**If you get "repository already exists" error:**
- The repository name is already taken. Choose a different name or use your existing repository.

**If push is rejected:**
- Make sure you have write access to the repository
- Verify the remote URL is correct: `git remote -v`

**If you need to change the remote URL:**
```bash
git remote set-url origin https://github.com/YOUR_USERNAME/kolesa_dwh.git
```

## Next Steps

After successfully pushing to GitHub:

1. Update the README.md with your actual repository URL
2. Consider adding a LICENSE file
3. Set up GitHub Actions for CI/CD (optional)
4. Add topics/tags to your repository for discoverability

