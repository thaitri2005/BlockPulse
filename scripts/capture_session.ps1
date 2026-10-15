# One data-collection session: capture -> process -> rule screen -> Postgres -> compact -> log.
# Run from your own PowerShell (the live feed needs your network). Needs Postgres running:
#   docker compose --profile m2 up -d --wait postgres
# Each session creates new directories and never overwrites earlier ones. For unattended hourly collection see
# scripts/install_collector.ps1. The Airflow DAG (orchestration/airflow) does the same steps.
param(
    [int]$Duration = 600,        # seconds. Use 3540 for the hourly collector.
    [string]$Url = "wss://node203.sv1.mempool.space/api/v1/ws",
    [string]$FromCapture,        # skip the live capture and reuse an existing messages.jsonl (for testing)
    [int]$MaxReconnects = 300,   # the limit is cumulative per capture and the feed drops every few minutes
    [double]$MinFreeGiB = 15,    # refuse to start a capture below this much free disk
    [string]$OutputRoot = "data",
    [string]$LogPath = "data/capture_log.csv"
)
$ErrorActionPreference = "Stop"
$env:PYTHONWARNINGS = "ignore"
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$name = "session-$stamp"

function Run($what, [string[]]$arguments) {
    $output = & $py -W ignore -m blockpulse @arguments 2>&1
    if ($LASTEXITCODE -ne 0) { throw "$what failed (exit $LASTEXITCODE): $(($output | Select-Object -Last 5) -join ' ')" }
}

if ($FromCapture) { $capture = $FromCapture; $dir = Split-Path $FromCapture -Parent }
else {
    $free = [math]::Round((Get-PSDrive -Name ((Resolve-Path $OutputRoot).Drive.Name)).Free / 1GB, 1)
    if ($free -lt $MinFreeGiB) { throw "Only $free GiB free (limit $MinFreeGiB); not starting a capture." }
    $dir = "$OutputRoot/raw/$name"
    Write-Host "Capturing for $Duration seconds into $dir ..."
    Run "capture" @("capture", "--duration", "$Duration", "--url", $Url, "--output", $dir,
                    "--max-reconnects", "$MaxReconnects", "--max-messages", "200000", "--max-bytes", "$(400MB)")
    $capture = "$dir/messages.jsonl"
}
$processed = "$OutputRoot/processed/$name"; $screened = "$OutputRoot/screened/$name"
Write-Host "Processing";   Run "process" @("process", $capture, "--output", $processed)
Write-Host "Rule screen";  Run "detect" @("detect", "$processed/features.jsonl", "--output", $screened)
Write-Host "Loading into Postgres"
Run "load-results" @("load-results", $screened, "--run-id", $name)
Run "load-events" @("load-events", $processed)

# Gzip the bulky events file after it is loaded (raw captures stay untouched as the source of truth).
$events = "$processed/events.jsonl"
if (Test-Path $events) {
    $in = [IO.File]::OpenRead((Resolve-Path $events)); $out = [IO.File]::Create("$((Resolve-Path $processed).Path)\events.jsonl.gz")
    $gz = New-Object IO.Compression.GZipStream($out, [IO.Compression.CompressionMode]::Compress)
    $in.CopyTo($gz); $gz.Dispose(); $out.Dispose(); $in.Dispose()
    Remove-Item $events
}

# Append a one-line record so we can see, later, what data exists and how complete it was.
$cap = if ($FromCapture) { $null } else { Get-Content "$dir/summary.json" -Raw | ConvertFrom-Json }
$proc = Get-Content "$processed/summary.json" -Raw | ConvertFrom-Json
$scr = Get-Content "$screened/summary.json" -Raw | ConvertFrom-Json
$row = [pscustomobject]@{
    session = $name; seconds = if ($cap) { $cap.elapsed_seconds } else { "replay" }
    messages = $proc.lines_read; features = $proc.features_written; flagged = $scr.flagged_rows
    connection_errors = if ($cap) { $cap.connection_errors } else { "" }; stop_reason = if ($cap) { $cap.stop_reason } else { "" }
}
New-Item -ItemType Directory -Force (Split-Path $LogPath -Parent) | Out-Null
$row | Export-Csv $LogPath -Append -NoTypeInformation
$row | Format-List
Write-Host "Logged to $LogPath. Dashboard: http://127.0.0.1:3000"
