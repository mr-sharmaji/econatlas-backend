#Requires -RunAsAdministrator
<#
.SYNOPSIS
    One-time setup for running econatlas-backend as a Windows service.

.DESCRIPTION
    This script:
      1. Creates the deployment directory (C:\econatlas-backend)
      2. Clones the repo and sets up a Python virtual environment
      3. Installs NSSM (Non-Sucking Service Manager) via winget
      4. Registers the app as a Windows service
      5. Walks you through creating the .env file
      6. Starts the service

    Run this script once on the Windows machine. After that, the GitHub Actions
    self-hosted runner handles all future deployments automatically.

.NOTES
    Prerequisites:
      - Windows 10/11 or Windows Server 2019+
      - Python 3.13+ installed and on PATH
      - Git installed and on PATH
      - winget available (or install NSSM manually)
#>

param(
    [string]$DeployDir   = "C:\econatlas-backend",
    [string]$RepoUrl     = "",                        # e.g. https://github.com/you/econatlas-backend.git
    [string]$ServiceName = "econatlas-backend",
    [int]$Port           = 8000
)

$ErrorActionPreference = "Stop"

function Write-Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

# ── 1. Validate prerequisites ──────────────────────────────────────────────

Write-Step "Checking prerequisites"

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) { throw "Python is not installed or not on PATH." }
$pyVer = & python --version 2>&1
Write-Host "  Python: $pyVer"

$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) { throw "Git is not installed or not on PATH." }
Write-Host "  Git:    $(git --version)"

# ── 2. Clone or update the repo ────────────────────────────────────────────

Write-Step "Setting up deployment directory: $DeployDir"

if (-not (Test-Path $DeployDir)) {
    if (-not $RepoUrl) {
        $RepoUrl = Read-Host "Enter the GitHub repo URL (e.g. https://github.com/user/econatlas-backend.git)"
    }
    Write-Host "  Cloning repository..."
    git clone $RepoUrl $DeployDir
} else {
    Write-Host "  Directory already exists — pulling latest..."
    Push-Location $DeployDir
    git pull
    Pop-Location
}

# ── 3. Create virtual environment ──────────────────────────────────────────

Write-Step "Creating Python virtual environment"

$venvDir = Join-Path $DeployDir ".venv"

if (-not (Test-Path (Join-Path $venvDir "Scripts\python.exe"))) {
    python -m venv $venvDir
    Write-Host "  Created .venv"
} else {
    Write-Host "  .venv already exists"
}

$pip = Join-Path $venvDir "Scripts\pip.exe"
& $pip install --upgrade pip --quiet
& $pip install -r (Join-Path $DeployDir "requirements.txt") --quiet
Write-Host "  Dependencies installed"

# ── 4. Create .env file ───────────────────────────────────────────────────

Write-Step "Configuring environment variables"

$envFile = Join-Path $DeployDir ".env"

if (-not (Test-Path $envFile)) {
    Write-Host "  No .env file found. Let's create one."
    $supaUrl = Read-Host "  SUPABASE_URL"
    $supaKey = Read-Host "  SUPABASE_KEY"
    $supaSvc = Read-Host "  SUPABASE_SERVICE_KEY"

    $envContent = "SUPABASE_URL=$supaUrl`nSUPABASE_KEY=$supaKey`nSUPABASE_SERVICE_KEY=$supaSvc"
    $envContent | Set-Content -Path $envFile -Encoding UTF8

    Write-Host "  .env created at $envFile"
} else {
    Write-Host "  .env already exists — skipping"
}

# ── 5. Install NSSM ───────────────────────────────────────────────────────

Write-Step "Installing NSSM (service manager)"

$nssm = Get-Command nssm -ErrorAction SilentlyContinue
if (-not $nssm) {
    Write-Host "  Installing via winget..."
    winget install --id nssm.nssm --accept-source-agreements --accept-package-agreements
    # Refresh PATH
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
                [System.Environment]::GetEnvironmentVariable("Path", "User")
    $nssm = Get-Command nssm -ErrorAction SilentlyContinue
    if (-not $nssm) {
        throw "NSSM installation failed. Install manually: https://nssm.cc/download"
    }
}
Write-Host "  NSSM: $(nssm version 2>&1 | Select-Object -First 1)"

# ── 6. Register the Windows service ───────────────────────────────────────

Write-Step "Registering Windows service: $ServiceName"

$existingStatus = nssm status $ServiceName 2>&1
if ($existingStatus -match "SERVICE_") {
    Write-Host "  Service already exists (status: $existingStatus)"
    $overwrite = Read-Host "  Overwrite? (y/N)"
    if ($overwrite -ne "y") {
        Write-Host "  Keeping existing service config"
    } else {
        nssm stop $ServiceName 2>&1 | Out-Null
        nssm remove $ServiceName confirm
    }
}

if (-not ($existingStatus -match "SERVICE_") -or $overwrite -eq "y") {
    $uvicorn = Join-Path $venvDir "Scripts\uvicorn.exe"

    nssm install $ServiceName $uvicorn
    nssm set $ServiceName AppParameters "app.main:app --host 0.0.0.0 --port $Port"
    nssm set $ServiceName AppDirectory $DeployDir
    nssm set $ServiceName AppEnvironmentExtra "PATH=$venvDir\Scripts;%PATH%"
    nssm set $ServiceName DisplayName "EconAtlas Backend API"
    nssm set $ServiceName Description "FastAPI backend for the EconAtlas economic intelligence system"
    nssm set $ServiceName Start SERVICE_AUTO_START
    nssm set $ServiceName AppStdout (Join-Path $DeployDir "logs\service-stdout.log")
    nssm set $ServiceName AppStderr (Join-Path $DeployDir "logs\service-stderr.log")
    nssm set $ServiceName AppRotateFiles 1
    nssm set $ServiceName AppRotateBytes 5242880

    New-Item -ItemType Directory -Path (Join-Path $DeployDir "logs") -Force | Out-Null
    Write-Host "  Service registered"
}

# ── 7. Start the service ──────────────────────────────────────────────────

Write-Step "Starting service"

nssm start $ServiceName
Start-Sleep -Seconds 3
$status = nssm status $ServiceName
Write-Host "  Status: $status"

if ($status -match "SERVICE_RUNNING") {
    Write-Host "`n  API is live at http://localhost:$Port" -ForegroundColor Green
    Write-Host "  Docs available at http://localhost:$Port/docs" -ForegroundColor Green
} else {
    Write-Host "`n  Service did not start. Check logs:" -ForegroundColor Yellow
    Write-Host "    $DeployDir\logs\service-stderr.log"
}

# ── 8. Next steps ─────────────────────────────────────────────────────────

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  SETUP COMPLETE -- Next: Install the GitHub Actions self-hosted runner" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  1. Go to your GitHub repo -> Settings -> Actions -> Runners" -ForegroundColor Cyan
Write-Host "  2. Click 'New self-hosted runner' and select Windows / x64" -ForegroundColor Cyan
Write-Host "  3. Follow the instructions to download and configure the runner" -ForegroundColor Cyan
Write-Host "  4. Install the runner as a service:" -ForegroundColor Cyan
Write-Host "" -ForegroundColor Cyan
Write-Host "       cd C:\actions-runner" -ForegroundColor Cyan
Write-Host "       .\svc.ps1 install" -ForegroundColor Cyan
Write-Host "       .\svc.ps1 start" -ForegroundColor Cyan
Write-Host "" -ForegroundColor Cyan
Write-Host "  Once the runner is active, every push to 'main' will automatically" -ForegroundColor Cyan
Write-Host "  deploy to this machine." -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
