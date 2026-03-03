# BlockID Nightly Pipeline Runner
# Run via: powershell -ExecutionPolicy Bypass -File run_blockid_pipeline.ps1
# Schedule: Windows Task Scheduler, Daily 00:00 Jakarta time

Set-Location D:\BACKENDBLOCKID

$env:BLOCKID_TEST_MODE = "0"
$env:BLOCKID_PIPELINE_MODE = "1"
$env:SOLANA_CLUSTER = "mainnet"

$timestamp = Get-Date -Format "yyyyMMdd_HHmm"
$logDir = "logs"
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}
$logfile = Join-Path $logDir "pipeline_$timestamp.log"

"BlockID pipeline started at $(Get-Date)" | Out-File -FilePath $logfile -Encoding utf8
py -m backend_blockid.tools.run_full_pipeline *>> $logfile

if ($LASTEXITCODE -ne 0) {
    "Pipeline FAILED at $(Get-Date) - exit code $LASTEXITCODE" | Add-Content -Path $logfile
    # Future: send Telegram/email alert
    exit $LASTEXITCODE
}

"Pipeline finished at $(Get-Date)" | Add-Content -Path $logfile
