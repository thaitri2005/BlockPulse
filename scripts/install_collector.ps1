# Install (or remove) a Windows scheduled task that collects data every hour with scripts/capture_session.ps1.
#   Install:  powershell -File scripts/install_collector.ps1
#   Remove:   powershell -File scripts/install_collector.ps1 -Remove
#   Check:    Get-ScheduledTask BlockPulseCollector | Get-ScheduledTaskInfo
# Each run captures for -Duration seconds (default 59 minutes) and then processes the data. A run's processing
# overlaps the next run's capture, so instances are allowed to run in parallel.
# This does NOT keep the laptop awake: while Windows sleeps, nothing is collected. Set sleep to "Never" while
# plugged in (Settings > System > Power) for a continuous run.
param(
    [switch]$Remove,
    [int]$Duration = 3540,
    [string]$TaskName = "BlockPulseCollector"
)
$ErrorActionPreference = "Stop"
if ($Remove) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed $TaskName. Already collected data stays in data/."
    return
}
$root = Split-Path $PSScriptRoot -Parent
$action = New-ScheduledTaskAction -Execute "powershell.exe" -WorkingDirectory $root `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$root\scripts\capture_session.ps1`" -Duration $Duration"
$start = (Get-Date).Date.AddHours((Get-Date).Hour + 1)  # next top of the hour
$trigger = New-ScheduledTaskTrigger -Once -At $start -RepetitionInterval (New-TimeSpan -Hours 1)
$settings = New-ScheduledTaskSettingsSet -MultipleInstances Parallel -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 2)
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Description "BlockPulse hourly mempool capture, processing, and load" -Force | Out-Null
Write-Host "Installed $TaskName. First run at $start, then hourly."
Write-Host "Postgres must be running: docker compose --profile m2 up -d --wait postgres"
Write-Host "Disk use is about 4 to 5 GB per day; captures stop when free space drops below 15 GiB."
