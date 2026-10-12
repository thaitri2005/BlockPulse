# Offline M2 demo: synthetic fixture -> features -> rule screen -> Postgres -> trace -> API.
# Uses a separate database (blockpulse_demo) so synthetic rows never mix with real results.
# Needs: pip install -e ".[m2,dev]" and the Postgres container (docker compose --profile m2 up -d --wait postgres).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$run = [guid]::NewGuid().ToString("N").Substring(0, 8)
$fixture = "examples/fixtures/mempool_messages.jsonl"

# CREATE DATABASE fails harmlessly when it already exists.
$ErrorActionPreference = "Continue"
docker compose --profile m2 exec -T postgres psql -U blockpulse -d blockpulse -c "CREATE DATABASE blockpulse_demo" 2>$null | Out-Null
$ErrorActionPreference = "Stop"
$env:BLOCKPULSE_DSN = "postgresql://blockpulse:blockpulse_dev@127.0.0.1:5432/blockpulse_demo"

Write-Host "1/6 Create tables";                    & $py -m blockpulse db-init | Out-Null
Write-Host "2/6 Process fixture -> features";       & $py -m blockpulse process $fixture --output "data/demo/processed-$run" | Out-Null
Write-Host "3/6 Rule screen";                       & $py -m blockpulse detect "data/demo/processed-$run/features.jsonl" --output "data/demo/screened-$run" | Out-Null
Write-Host "4/6 Load scores and lifecycle events";  & $py -m blockpulse load-results "data/demo/screened-$run" --run-id "demo-$run" | Out-Null
                                                    & $py -m blockpulse load-events "data/demo/processed-$run" | Out-Null
$txid = (Get-Content "data/demo/processed-$run/features.jsonl" -TotalCount 1 | ConvertFrom-Json).txid
Write-Host "5/6 Trace $txid"
& $py -m blockpulse trace $txid --capture $fixture --brief
Write-Host "6/6 API: run   `$env:BLOCKPULSE_DSN='$env:BLOCKPULSE_DSN'; $py -m blockpulse serve   then open http://127.0.0.1:8000/docs"
