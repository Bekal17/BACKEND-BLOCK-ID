# Create BlockID Nightly Pipeline Windows Task
# Run as Administrator: powershell -ExecutionPolicy Bypass -File scripts/create_nightly_task.ps1
# Jakarta time = UTC+7. 00:00 Jakarta = 17:00 UTC (previous day)

$taskName = "BlockID Nightly Pipeline"
$scriptPath = "D:\BACKENDBLOCKID\run_blockid_pipeline.ps1"
$workDir = "D:\BACKENDBLOCKID"

# Daily at 00:00 (local time = Jakarta if system TZ set to Asia/Jakarta)
$trigger = New-ScheduledTaskTrigger -Daily -At "00:00"
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -File `"$scriptPath`"" `
    -WorkingDirectory $workDir
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -RunOnlyIfNetworkAvailable $false -StartWhenAvailable

Register-ScheduledTask -TaskName $taskName -Trigger $trigger -Action $action -Settings $settings -Description "BlockID pipeline: graph, flow, drainer, scoring, publish"

Write-Output "Task '$taskName' created. Run whether user logged on: edit in Task Scheduler GUI."
Write-Output "Trigger: Daily 00:00 | Action: powershell -File run_blockid_pipeline.ps1"
