# BlockID Nightly Pipeline Scheduler

## Quick Start

### 1. Test manually
```powershell
cd D:\BACKENDBLOCKID
powershell -ExecutionPolicy Bypass -File run_blockid_pipeline.ps1
```

### 2. Check logs
```
D:\BACKENDBLOCKID\logs\pipeline_yyyyMMdd_HHmm.log
```

### 3. Create Windows Task (manual)

1. Open **Task Scheduler** → Create Basic Task
2. **Name:** BlockID Nightly Pipeline
3. **Trigger:** Daily → 00:00 (Jakarta time)
4. **Action:** Start Program
   - Program: `powershell.exe`
   - Arguments: `-ExecutionPolicy Bypass -File "D:\BACKENDBLOCKID\run_blockid_pipeline.ps1"`
   - Start in: `D:\BACKENDBLOCKID`
5. **Settings:**
   - Run whether user logged on or not
   - Run with highest privileges

### 4. Create via script (optional)
```powershell
powershell -ExecutionPolicy Bypass -File scripts/create_nightly_task.ps1
```

## Failure alert
On pipeline failure, the script writes "Pipeline FAILED" to the log.
Future: Telegram/email alert.

## Future upgrades
- Telegram alert
- Email alert
- Retry on failure
- Run only incremental wallets
- Parallel wallet batches
