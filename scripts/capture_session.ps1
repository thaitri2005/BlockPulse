# One bounded data-collection session: capture -> process -> rule screen -> Postgres.
# Run from your own PowerShell (the live feed needs your network). Needs Postgres running:
#   docker compose --profile m2 up -d --wait postgres
# Each session creates new directories under data/ and never overwrites earlier ones.
param(
    [int]$Duration = 600,   # seconds; hard-capped by the capture's own 50 MiB / 10,000-message limits
    [string]$Url = "wss://node203.sv1.mempool.space/api/v1/ws",
    [string]$FromCapture    # skip the live capture and reuse an existing messages.jsonl (for testing)
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")

if ($FromCapture) { $capture = $FromCapture }
else {
    $dir = "data/raw/session-$stamp"
    Write-Host "Capturing for $Duration seconds into $dir ..."
    & $py -m blockpulse capture --duration $Duration --url $Url --output $dir | Out-Null
    $capture = "$dir/messages.jsonl"
}
Write-Host "Processing";    & $py -m blockpulse process $capture --output "data/processed/session-$stamp" | Out-Null
Write-Host "Rule screen";   & $py -m blockpulse detect "data/processed/session-$stamp/features.jsonl" --output "data/screened/session-$stamp" | Out-Null
Write-Host "Loading into Postgres"
& $py -m blockpulse load-results "data/screened/session-$stamp" --run-id "session-$stamp" | Out-Null
& $py -m blockpulse load-events "data/processed/session-$stamp" | Out-Null

# Append a one-line record so we can see, later, what data exists and how complete it was.
$cap = if ($FromCapture) { $null } else { Get-Content "$dir/summary.json" -Raw | ConvertFrom-Json }
$proc = Get-Content "data/processed/session-$stamp/summary.json" -Raw | ConvertFrom-Json
$scr = Get-Content "data/screened/session-$stamp/summary.json" -Raw | ConvertFrom-Json
$row = [pscustomobject]@{
    session = "session-$stamp"; seconds = if ($cap) { $cap.elapsed_seconds } else { "replay" }
    messages = $proc.lines_read; features = $proc.features_written; flagged = $scr.flagged_rows
    connection_errors = if ($cap) { $cap.connection_errors } else { "" }; stop_reason = if ($cap) { $cap.stop_reason } else { "" }
}
New-Item -ItemType Directory -Force data | Out-Null
$row | Export-Csv data/capture_log.csv -Append -NoTypeInformation
$row | Format-List
Write-Host "Logged to data/capture_log.csv. Dashboard: http://127.0.0.1:3000"
