# One data-collection session on this laptop: capture, then process, screen, load into Postgres, compact, log.
# A thin wrapper around `blockpulse collect`; the same code runs on the AWS instance (see docs/AWS_DEPLOY.md).
# Needs Postgres running:  docker compose --profile m2 up -d --wait postgres
# Run from your own PowerShell (the live feed needs your network). Hourly unattended use: scripts/install_collector.ps1
param(
    [int]$Duration = 600,       # seconds; the hourly task uses 3540
    [string]$Url = "wss://node203.sv1.mempool.space/api/v1/ws",
    [double]$MinFreeGiB = 15,   # refuse to start a capture below this much free disk
    [switch]$NoDatabase         # skip the Postgres load (data is still processed and can be loaded later)
)
$ErrorActionPreference = "Stop"
$env:PYTHONWARNINGS = "ignore"
Set-Location (Split-Path $PSScriptRoot -Parent)
$arguments = @("-m", "blockpulse", "collect", "--root", "data", "--duration", "$Duration", "--url", $Url, "--min-free-gib", "$MinFreeGiB")
if (-not $NoDatabase) { $arguments += "--load-db" }
& .\.venv\Scripts\python.exe @arguments
exit $LASTEXITCODE
