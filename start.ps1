# Launch the Gently Annotator server.
#
# Usage:
#   .\start.ps1                  # uses host/port from config.yaml
#   .\start.ps1 --port 8091      # any extra args are passed through to
#                                # annotator.server (see --help for the list)
#
# Reads the venv at ./venv/. If it doesn't exist, exits with a hint on
# how to bootstrap it.

$ErrorActionPreference = "Stop"
$repo = $PSScriptRoot
$py = Join-Path $repo "venv\Scripts\python.exe"

if (-not (Test-Path $py)) {
    Write-Host "venv not found at: $py" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Bootstrap it once with:" -ForegroundColor Yellow
    Write-Host "  python -m venv venv"
    Write-Host "  .\venv\Scripts\python.exe -m pip install -e ."
    exit 1
}

Push-Location $repo
try {
    & $py -m annotator.server @args
} finally {
    Pop-Location
}
