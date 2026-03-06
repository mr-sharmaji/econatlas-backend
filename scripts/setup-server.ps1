#Requires -RunAsAdministrator

param(
    [string]$DeployDir   = "C:\econatlas-backend",
    [string]$RepoUrl     = "",
    [string]$ServiceName = "econatlas-backend",
    [int]$Port           = 8000
)

$ErrorActionPreference = "Stop"

function Write-Step($msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

# ---------- 1. Validate prerequisites ----------

Write-Step "Checking prerequisites"

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
    throw "Python is not installed or not on PATH."
}
$pyVer = & python --version 2>&1
Write-Host "  Python: $pyVer"

$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    throw "Git is not installed or not on PATH."
}
Write-Host "  Git:    $(git --version)"

# ---------- 2. Clone or update the repo ----------

Write-Step "Setting up deployment directory: $DeployDir"

if (-not (Test-Path $DeployDir)) {
    if (-not $RepoUrl) {
        $RepoUrl = Read-Host "Enter the GitHub repo URL"
    }
    Write-Host "  Cloning repository..."
    git clone $RepoUrl $DeployDir
}
else {
    Write-Host "  Directory already exists - pulling latest..."
    Push-Location $DeployDir
    git pull
    Pop-Location
}

# ---------- 3. Create virtual environment ----------

Write-Step "Creating Python virtual environment"

$venvDir = Join-Path $DeployDir ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
    python -m venv $venvDir
    Write-Host "  Created .venv"
}
else {
    Write-Host "  .venv already exists"
}

$pip = Join-Path $venvDir "Scripts\pip.exe"
$reqFile = Join-Path $DeployDir "requirements.txt"
& $pip install --upgrade pip --quiet
& $pip install -r $reqFile --quiet
Write-Host "  Dependencies installed"

# ---------- 4. Create .env file ----------

Write-Step "Configuring environment variables"

$envFile = Join-Path $DeployDir ".env"

if (-not (Test-Path $envFile)) {
    Write-Host "  No .env file found. Creating one now."
    $supaUrl = Read-Host "  SUPABASE_URL"
    $supaKey = Read-Host "  SUPABASE_KEY"
    $supaSvc = Read-Host "  SUPABASE_SERVICE_KEY"

    $lines = @(
        "SUPABASE_URL=$supaUrl",
        "SUPABASE_KEY=$supaKey",
        "SUPABASE_SERVICE_KEY=$supaSvc"
    )
    $lines | Set-Content -Path $envFile -Encoding UTF8
    Write-Host "  .env created at $envFile"
}
else {
    Write-Host "  .env already exists - skipping"
}

# ---------- 5. Install NSSM ----------

Write-Step "Installing NSSM (service manager)"

$nssmCmd = Get-Command nssm -ErrorAction SilentlyContinue
if (-not $nssmCmd) {
    Write-Host "  Installing via winget..."
    winget install --id nssm.nssm --accept-source-agreements --accept-package-agreements
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
    $nssmCmd = Get-Command nssm -ErrorAction SilentlyContinue
    if (-not $nssmCmd) {
        throw "NSSM installation failed. Install manually from https://nssm.cc/download"
    }
}
Write-Host "  NSSM found"

# ---------- 6. Register the Windows service ----------

Write-Step "Registering Windows service: $ServiceName"

$existingStatus = nssm status $ServiceName 2>&1
$serviceExists = $existingStatus -match "SERVICE_"
$doInstall = $true

if ($serviceExists) {
    Write-Host "  Service already exists (status: $existingStatus)"
    $overwrite = Read-Host "  Overwrite? (y/N)"
    if ($overwrite -eq "y") {
        nssm stop $ServiceName 2>&1 | Out-Null
        nssm remove $ServiceName confirm
    }
    else {
        $doInstall = $false
        Write-Host "  Keeping existing service config"
    }
}

if ($doInstall) {
    $uvicorn = Join-Path $venvDir "Scripts\uvicorn.exe"
    $logsDir = Join-Path $DeployDir "logs"

    nssm install $ServiceName $uvicorn
    nssm set $ServiceName AppParameters "app.main:app --host 0.0.0.0 --port $Port"
    nssm set $ServiceName AppDirectory $DeployDir
    nssm set $ServiceName AppEnvironmentExtra "PATH=$venvDir\Scripts;%PATH%"
    nssm set $ServiceName DisplayName "EconAtlas Backend API"
    nssm set $ServiceName Description "FastAPI backend for EconAtlas"
    nssm set $ServiceName Start SERVICE_AUTO_START
    nssm set $ServiceName AppStdout (Join-Path $logsDir "service-stdout.log")
    nssm set $ServiceName AppStderr (Join-Path $logsDir "service-stderr.log")
    nssm set $ServiceName AppRotateFiles 1
    nssm set $ServiceName AppRotateBytes 5242880

    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
    Write-Host "  Service registered"
}

# ---------- 7. Start the service ----------

Write-Step "Starting service"

nssm start $ServiceName
Start-Sleep -Seconds 3
$status = nssm status $ServiceName
Write-Host "  Status: $status"

if ($status -match "SERVICE_RUNNING") {
    Write-Host ""
    Write-Host "  API is live at http://localhost:$Port" -ForegroundColor Green
    Write-Host "  Docs available at http://localhost:$Port/docs" -ForegroundColor Green
}
else {
    Write-Host ""
    Write-Host "  Service did not start. Check logs:" -ForegroundColor Yellow
    Write-Host "    $DeployDir\logs\service-stderr.log"
}

# ---------- 8. Next steps ----------

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  SETUP COMPLETE -- Next: Install the GitHub Actions self-hosted runner" -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  1. Go to your GitHub repo -> Settings -> Actions -> Runners" -ForegroundColor Cyan
Write-Host "  2. Click 'New self-hosted runner' and select Windows / x64" -ForegroundColor Cyan
Write-Host "  3. Follow the instructions to download and configure the runner" -ForegroundColor Cyan
Write-Host "  4. Install the runner as a service:" -ForegroundColor Cyan
Write-Host ""
Write-Host "       cd C:\actions-runner" -ForegroundColor Cyan
Write-Host "       .\svc.ps1 install" -ForegroundColor Cyan
Write-Host "       .\svc.ps1 start" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Once the runner is active, every push to main will automatically" -ForegroundColor Cyan
Write-Host "  deploy to this machine." -ForegroundColor Cyan
Write-Host "================================================================================" -ForegroundColor Cyan
