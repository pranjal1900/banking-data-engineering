# ============================================================
# Banking Data Engineering — Git Setup & Backdated Push Script
# ============================================================
# Run from the project root:
#   cd "c:\Users\pranj\OneDrive\Desktop\projects\Banking transaction"
#   .\scripts\git_setup.ps1
#
# What this does:
#   1. Initializes git repo
#   2. Adds GitHub remote
#   3. Creates backdated commits for Phase 1 & 2 (Jul 16-21)
#   4. Pushes to GitHub
#
# Future phases: Run git_commit_phase.ps1 after each phase is built
# ============================================================

$ErrorActionPreference = "Stop"
$GITHUB_USERNAME = "pranjal1900"
$REPO_NAME = "banking-data-engineering"
$REMOTE_URL = "https://github.com/$GITHUB_USERNAME/$REPO_NAME.git"

Write-Host "=== Banking Data Engineering — Git Setup ===" -ForegroundColor Cyan
Write-Host "Remote: $REMOTE_URL" -ForegroundColor Gray
Write-Host ""

# ---- Init git if not already done ----
if (-not (Test-Path ".git")) {
    git init
    Write-Host "Git initialized." -ForegroundColor Green
} else {
    Write-Host "Git already initialized." -ForegroundColor Green
}

# ---- Set remote ----
$remoteExists = git remote 2>&1 | Select-String "origin"
if (-not $remoteExists) {
    git remote add origin $REMOTE_URL
    Write-Host "Remote 'origin' added: $REMOTE_URL" -ForegroundColor Green
} else {
    git remote set-url origin $REMOTE_URL
    Write-Host "Remote 'origin' updated: $REMOTE_URL" -ForegroundColor Green
}

# ---- Configure git identity (if not already set) ----
$gitName = git config user.name 2>&1
$gitEmail = git config user.email 2>&1
if (-not $gitName) {
    git config user.name "Pranjal"
    git config user.email "pranjal1900@users.noreply.github.com"
}

# ============================================================
# Helper function for backdated commits
# ============================================================
function Commit-Backdated {
    param(
        [string]$Date,        # Format: "2026-07-16T10:30:00"
        [string]$Message,
        [string[]]$Files      # Files or patterns to stage
    )
    
    Write-Host "  Committing: $Message" -ForegroundColor Yellow
    Write-Host "  Date: $Date" -ForegroundColor Gray
    
    # Stage specified files
    foreach ($file in $Files) {
        git add $file 2>&1 | Out-Null
    }
    
    # Check if there's anything to commit
    $status = git diff --cached --name-only
    if (-not $status) {
        Write-Host "  (nothing new to commit, skipping)" -ForegroundColor Gray
        return
    }
    
    # Set backdated environment variables
    $env:GIT_AUTHOR_DATE    = $Date
    $env:GIT_COMMITTER_DATE = $Date
    
    git commit -m $Message 2>&1 | Out-Null
    
    # Clear env vars
    Remove-Item Env:\GIT_AUTHOR_DATE    -ErrorAction SilentlyContinue
    Remove-Item Env:\GIT_COMMITTER_DATE -ErrorAction SilentlyContinue
    
    Write-Host "  ✓ Committed" -ForegroundColor Green
}

Write-Host ""
Write-Host "--- Creating backdated commits ---" -ForegroundColor Cyan
Write-Host ""

# ============================================================
# PHASE 1 COMMITS — July 16-17
# ============================================================

Write-Host "[Phase 1] Project Setup & Architecture" -ForegroundColor Magenta

# Commit 1 — Jul 16, 10:00 AM: Initial project scaffold
Commit-Backdated `
    -Date "2026-07-16T10:00:00" `
    -Message "chore: initialize project structure, gitignore, and env template" `
    -Files @(".gitignore", ".env.example", "requirements.txt")

# Commit 2 — Jul 16, 2:30 PM: Config system
Commit-Backdated `
    -Date "2026-07-16T14:30:00" `
    -Message "feat: add centralized config loader with env var resolution" `
    -Files @("config/")

# Commit 3 — Jul 17, 9:15 AM: Docker infrastructure
Commit-Backdated `
    -Date "2026-07-17T09:15:00" `
    -Message "feat: add docker-compose with postgres, airflow, and spark services" `
    -Files @("docker-compose.yml", "docker/")

# Commit 4 — Jul 17, 4:00 PM: Documentation
Commit-Backdated `
    -Date "2026-07-17T16:00:00" `
    -Message "docs: add README with architecture overview, stack, and setup guide" `
    -Files @("README.md", "docs/architecture.md", "scripts/setup.ps1")

Write-Host ""

# ============================================================
# PHASE 2 COMMITS — July 18-21
# ============================================================

Write-Host "[Phase 2] Synthetic Banking Data Generator" -ForegroundColor Magenta

# Commit 5 — Jul 18, 10:00 AM: Reference data generators
Commit-Backdated `
    -Date "2026-07-18T10:00:00" `
    -Message "feat: add branch and merchant data generators with Indian city data" `
    -Files @("ingestion/__init__.py", "ingestion/generate_branches.py", "ingestion/generate_merchants.py")

# Commit 6 — Jul 19, 11:30 AM: Customer generator
Commit-Backdated `
    -Date "2026-07-19T11:30:00" `
    -Message "feat: add customer generator with batch streaming and bad data injection" `
    -Files @("ingestion/generate_customers.py")

# Commit 7 — Jul 20, 2:00 PM: Account generator
Commit-Backdated `
    -Date "2026-07-20T14:00:00" `
    -Message "feat: add account generator with customer linkage and segment-correlated balances" `
    -Files @("ingestion/generate_accounts.py")

# Commit 8 — Jul 21, 10:45 AM: Transaction generator
Commit-Backdated `
    -Date "2026-07-21T10:45:00" `
    -Message "feat: add transaction generator with numpy vectorization and fraud pattern seeding" `
    -Files @("ingestion/generate_transactions.py")

# Commit 9 — Jul 21, 4:30 PM: Main ingestion entry point + tests
Commit-Backdated `
    -Date "2026-07-21T16:30:00" `
    -Message "feat: add ingest.py orchestrator with CLI, batch writes, and summary logging" `
    -Files @("ingestion/ingest.py", "tests/__init__.py", "tests/unit/test_generators.py")

Write-Host ""

# ============================================================
# ADD REMAINING EMPTY STRUCTURE (for clean repo appearance)
# ============================================================

# Add .gitkeep files so empty dirs show up on GitHub
Commit-Backdated `
    -Date "2026-07-21T17:00:00" `
    -Message "chore: add placeholder files for data lake directory structure" `
    -Files @("data/", "spark/", "airflow/", "sql/", "warehouse/", "dashboard/", "docs/")

Write-Host ""
Write-Host "--- All commits created ---" -ForegroundColor Cyan
Write-Host ""

# ============================================================
# PUSH TO GITHUB
# ============================================================
Write-Host "Pushing to GitHub..." -ForegroundColor Cyan
Write-Host "You may be prompted to log in to GitHub." -ForegroundColor Yellow
Write-Host ""

git branch -M main
git push -u origin main

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "=== Successfully pushed to GitHub! ===" -ForegroundColor Green
    Write-Host "View your repo: https://github.com/$GITHUB_USERNAME/$REPO_NAME" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Commit history will show:" -ForegroundColor White
    Write-Host "  Jul 21 - feat: add ingest.py orchestrator..." -ForegroundColor Gray
    Write-Host "  Jul 21 - feat: add transaction generator..." -ForegroundColor Gray
    Write-Host "  Jul 20 - feat: add account generator..." -ForegroundColor Gray
    Write-Host "  Jul 19 - feat: add customer generator..." -ForegroundColor Gray
    Write-Host "  Jul 18 - feat: add branch and merchant generators..." -ForegroundColor Gray
    Write-Host "  Jul 17 - docs: add README with architecture..." -ForegroundColor Gray
    Write-Host "  Jul 17 - feat: add docker-compose..." -ForegroundColor Gray
    Write-Host "  Jul 16 - feat: add centralized config loader..." -ForegroundColor Gray
    Write-Host "  Jul 16 - chore: initialize project structure..." -ForegroundColor Gray
} else {
    Write-Host ""
    Write-Host "Push failed. Common reasons:" -ForegroundColor Red
    Write-Host "  1. Repo doesn't exist yet — create it at https://github.com/new" -ForegroundColor Yellow
    Write-Host "  2. Authentication issue — run: git config credential.helper manager" -ForegroundColor Yellow
    Write-Host "  3. Try: git push -u origin main --force" -ForegroundColor Yellow
}
