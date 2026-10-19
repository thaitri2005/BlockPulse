# Rebuild the M3 chain from a dataset manifest and compare it with the recorded artifacts.
# Writes only under data/repro/<stamp>/ and changes nothing else. Needs: pip install -e ".[ml]"
# Usage: powershell -File scripts/reproduce_m3.ps1 [-Manifest data/datasets/dataset-<id>.json] [-Seed 7] [-Budget 25] [-SweepSeed 11]
param(
    [string]$Manifest = "data/datasets/dataset-acc0c2dc3985.json",
    [int]$Seed = 7,
    [double]$Budget = 25,
    [int]$SweepSeed = 11
)
$ErrorActionPreference = "Stop"
$env:PYTHONWARNINGS = "ignore"
Set-Location (Split-Path $PSScriptRoot -Parent)
$py = ".\.venv\Scripts\python.exe"
$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$out = "data/repro/$stamp"
$id = (Split-Path $Manifest -Leaf) -replace "^dataset-|\.json$", ""
$modelName = "iforest-$id-seed$Seed"
$evalName = "$modelName-budget$Budget"
$sweepName = "$evalName-seed$SweepSeed"

function Read-Json($path) { Get-Content $path -Raw | ConvertFrom-Json }
$checks = @()
function Check($name, $recorded, $rebuilt) {
    $script:checks += [pscustomobject]@{ check = $name; matches = ("$recorded" -eq "$rebuilt"); recorded = "$recorded"; rebuilt = "$rebuilt" }
}

# 1. The manifest: its ID must match its content and every screened file it names must be unchanged.
#    (Rebuilding from the folder would pick up sessions collected later and give a different manifest.)
New-Item -ItemType Directory -Force $out | Out-Null
& $py -m blockpulse verify-dataset $Manifest > "$out/verify.json"
Check "manifest_intact" "True" ((Get-Content "$out/verify.json" -Raw | ConvertFrom-Json).ok)

# 2. The model, trained again from the manifest with the same seed.
& $py -W ignore -m blockpulse train-iforest $Manifest --seed $Seed --output "$out/$modelName" | Out-Null
Check "model_sha256" (Read-Json "data/models/$modelName/metadata.json").model_sha256 (Read-Json "$out/$modelName/metadata.json").model_sha256

# 3. The calibrated threshold and detector ID.
& $py -W ignore -m blockpulse evaluate-iforest $Manifest "$out/$modelName" --alerts-per-1000 $Budget --output "$out/$evalName" | Out-Null
$recordedEval = Read-Json "data/evaluations/$evalName/summary.json"
$rebuiltEval = Read-Json "$out/$evalName/summary.json"
Check "detector_id" $recordedEval.detector_id $rebuiltEval.detector_id
Check "threshold" $recordedEval.threshold.threshold $rebuiltEval.threshold.threshold
Check "test_model_flagged" $recordedEval.test.model_flagged $rebuiltEval.test.model_flagged

# 4. The synthetic sweep.
& $py -W ignore -m blockpulse sweep $Manifest "$out/$modelName" "$out/$evalName" --seed $SweepSeed --output "$out/$sweepName" | Out-Null
Check "sweep_cases_sha256" (Read-Json "data/sweeps/$sweepName/summary.json").cases_sha256 (Read-Json "$out/$sweepName/summary.json").cases_sha256

$checks | Format-Table -AutoSize
$failed = @($checks | Where-Object { -not $_.matches }).Count
if ($failed -eq 0) { Write-Host "Reproduced: all $($checks.Count) checks match. Rebuilt files are in $out" }
else { Write-Host "$failed of $($checks.Count) checks differ. Compare the files in $out with the recorded ones."; exit 4 }
