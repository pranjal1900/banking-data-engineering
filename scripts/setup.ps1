# ============================================================
# Banking Data Engineering Platform — Windows Setup Script
# ============================================================
# Run from the project root:
#   .\scripts\setup.ps1
# ============================================================

Write-Host "=== Banking Data Engineering Platform Setup ===" -ForegroundColor Cyan
Write-Host ""

# ---- Check Python ----
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Python not found. Install from https://python.org" -ForegroundColor Red
    exit 1
}
Write-Host "Python: $pythonVersion" -ForegroundColor Green

# ---- Check Docker ----
$dockerVersion = docker --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Docker not found." -ForegroundColor Yellow
    Write-Host "         Download from: https://www.docker.com/products/docker-desktop/" -ForegroundColor Yellow
    Write-Host "         Docker is required for PostgreSQL and Airflow." -ForegroundColor Yellow
} else {
    Write-Host "Docker: $dockerVersion" -ForegroundColor Green
}

# ---- Check Java ----
$javaVersion = java -version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Java not found." -ForegroundColor Yellow
    Write-Host "         Download JDK 17 from: https://adoptium.net/temurin/releases/?version=17" -ForegroundColor Yellow
    Write-Host "         Java is required for PySpark." -ForegroundColor Yellow
} else {
    Write-Host "Java: $javaVersion" -ForegroundColor Green
}

# ---- Create .env if missing ----
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host ".env created from .env.example" -ForegroundColor Green
    Write-Host "  Edit .env to set your database password and other settings." -ForegroundColor Yellow
} else {
    Write-Host ".env already exists (skipping)" -ForegroundColor Green
}

# ---- Create virtual environment ----
if (-not (Test-Path "venv")) {
    Write-Host "Creating Python virtual environment..." -ForegroundColor Cyan
    python -m venv venv
    Write-Host "Virtual environment created." -ForegroundColor Green
} else {
    Write-Host "Virtual environment already exists (skipping)" -ForegroundColor Green
}

# ---- Activate and install packages ----
Write-Host "Installing Python dependencies..." -ForegroundColor Cyan
& "venv\Scripts\pip" install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Package installation failed." -ForegroundColor Red
    exit 1
}
Write-Host "Dependencies installed." -ForegroundColor Green

# ---- Create necessary directories ----
$dirs = @(
    "data\raw\customers", "data\raw\accounts", "data\raw\transactions",
    "data\raw\merchants", "data\raw\branches",
    "data\processed", "data\curated", "data\rejected", "data\sample",
    "logs"
)
foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}
Write-Host "Data directories created." -ForegroundColor Green

Write-Host ""
Write-Host "=== Setup Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Activate venv:        venv\Scripts\activate" -ForegroundColor Yellow
Write-Host "  2. Generate data:        python ingestion\ingest.py --mode dev" -ForegroundColor Yellow
Write-Host "  3. Run tests:            venv\Scripts\pytest tests\unit\ -v" -ForegroundColor Yellow
Write-Host "  4. Start infrastructure: docker compose up -d" -ForegroundColor Yellow
Write-Host ""
