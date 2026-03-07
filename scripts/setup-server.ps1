#Requires -RunAsAdministrator
# Sets up EconAtlas backend on Windows using Docker Compose (PostgreSQL + FastAPI app).

param(
    [string]$DeployDir = "C:\econatlas-backend",
    [string]$RepoUrl   = "",
    [int]$Port         = 8000
)

$ErrorActionPreference = "Stop"

function Write-Step($msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}

# ---------- 1. Validate prerequisites (install Docker if missing) ----------

Write-Step "Checking prerequisites"

function Refresh-EnvPath {
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
}

function Install-DockerDesktop {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        throw "winget is not available. Install Docker Desktop manually: https://docs.docker.com/desktop/install/windows-install/"
    }
    Write-Host "  Installing Docker Desktop via winget..."
    winget install --id Docker.DockerDesktop -e --accept-source-agreements --accept-package-agreements
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop installation failed. Install manually: https://docs.docker.com/desktop/install/windows-install/"
    }
    Refresh-EnvPath
}

# Ensure Docker is installed
$dockerMissing = -not (Get-Command docker -ErrorAction SilentlyContinue)
if ($dockerMissing) {
    Install-DockerDesktop
    # PATH may not include docker until a new shell is opened
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        Write-Host ""
        Write-Host "  Docker Desktop is installed. Open a new PowerShell window (as Administrator) and run this script again." -ForegroundColor Yellow
        Write-Host "  If Docker Desktop prompts to enable WSL2 or restart, complete that first." -ForegroundColor Yellow
        exit 1
    }
}

# Docker must be runnable (daemon)
try {
    $dockerVersion = docker --version 2>&1
    if ($LASTEXITCODE -ne 0) { throw "docker --version failed" }
    Write-Host "  Docker: $dockerVersion"
} catch {
    Write-Host "  Docker is installed but the daemon is not running. Starting Docker Desktop..." -ForegroundColor Yellow
    $dockerDesktop = "${env:ProgramFiles}\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerDesktop) {
        Start-Process -FilePath $dockerDesktop -WindowStyle Hidden
        Write-Host "  Waiting 30s for Docker Desktop to start..."
        Start-Sleep -Seconds 30
    }
    $dockerVersion = docker --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker daemon is not running. Start Docker Desktop from the Start menu, wait until it is ready, then run this script again."
    }
    Write-Host "  Docker: $dockerVersion"
}

# Docker Compose: prefer "docker compose" (V2), then "docker-compose" (standalone)
$script:ComposeCmd = $null
$composeV2 = docker compose version 2>&1
if ($LASTEXITCODE -eq 0) {
    $script:ComposeCmd = "docker compose"
    Write-Host "  Docker Compose: $composeV2"
} else {
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        $composeV1 = docker-compose --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            $script:ComposeCmd = "docker-compose"
            Write-Host "  Docker Compose: $composeV1"
        }
    }
}
if (-not $script:ComposeCmd) {
    Write-Host "  Docker Compose not found. Docker Desktop includes it; installing/repairing Docker Desktop..." -ForegroundColor Yellow
    Install-DockerDesktop
    $composeV2 = docker compose version 2>&1
    if ($LASTEXITCODE -eq 0) {
        $script:ComposeCmd = "docker compose"
        Write-Host "  Docker Compose: $composeV2"
    }
}
if (-not $script:ComposeCmd) {
    Write-Host ""
    Write-Host "  Open a new PowerShell window and run this script again so Docker Compose is on PATH." -ForegroundColor Yellow
    exit 1
}

# Git: install via winget if missing
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    if (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Host "  Installing Git via winget..."
        winget install --id Git.Git -e --accept-source-agreements --accept-package-agreements
        Refresh-EnvPath
    }
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        throw "Git is not installed and could not be installed. Install manually: https://git-scm.com/download/win"
    }
}
Write-Host "  Git: $(git --version)"

# ---------- 2. Clone or update the repo ----------

Write-Step "Setting up deployment directory: $DeployDir"

if (-not (Test-Path $DeployDir)) {
    if (-not $RepoUrl) {
        $RepoUrl = Read-Host "Enter the GitHub repo URL"
    }
    Write-Host "  Cloning repository..."
    git clone $RepoUrl $DeployDir
} else {
    Write-Host "  Directory already exists - pulling latest..."
    Push-Location $DeployDir
    git pull
    Pop-Location
}

# ---------- 3. Create .env if missing ----------

Write-Step "Configuring environment"

$envFile = Join-Path $DeployDir ".env"
if (-not (Test-Path $envFile)) {
    Write-Host "  No .env file found. Creating from .env.example..."
    $example = Join-Path $DeployDir ".env.example"
    if (Test-Path $example) {
        Copy-Item $example $envFile
        Write-Host "  .env created. Edit $envFile to set DATABASE_URL if not using Docker Compose."
    } else {
        $defaultUrl = "postgresql://econatlas:econatlas@localhost:5432/econatlas"
        Set-Content -Path $envFile -Value "DATABASE_URL=$defaultUrl"
        Write-Host "  .env created with default DATABASE_URL (for use with Docker Compose)."
    }
} else {
    Write-Host "  .env already exists - skipping"
}

# ---------- 4. Build and start with Docker Compose ----------

Write-Step "Building and starting containers"

Push-Location $DeployDir
try {
    Write-Host "  Using: $script:ComposeCmd"
    Invoke-Expression "$script:ComposeCmd build"
    Invoke-Expression "$script:ComposeCmd up -d"
    Write-Host "  Containers started"
    Invoke-Expression "$script:ComposeCmd ps"
} finally {
    Pop-Location
}

# ---------- 5. Summary ----------

Write-Host ""
Write-Host "  API is at http://localhost:$Port" -ForegroundColor Green
Write-Host "  Docs at http://localhost:$Port/docs" -ForegroundColor Green
Write-Host ""
Write-Host "  Manage: cd $DeployDir; docker compose ps | logs | down" -ForegroundColor Yellow
