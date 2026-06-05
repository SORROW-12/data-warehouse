$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$PYTHON = "C:\Users\26817\Desktop\data-analyst-ai\.venv\Scripts\python.exe"
$LOG_DIR = "logs"
$DATE = Get-Date -Format "yyyyMMdd_HHmmss"
New-Item -ItemType Directory -Force -Path $LOG_DIR | Out-Null
$steps = @("src\ods\ods_load.py","src\dwd\dwd_clean.py","src\dws\dws_agg.py","src\ads\ads_report.py")
$names = @("ODS","DWD","DWS","ADS")
Write-Host "Pipeline Start: $DATE"
for ($i=0; $i -lt $steps.Length; $i++) {
    $log = "$LOG_DIR\$($names[$i])_$DATE.log"
    Write-Host "[$( Get-Date -Format HH:mm:ss)] $($names[$i]) starting..."
    $proc = Start-Process -FilePath $PYTHON -ArgumentList $steps[$i] -RedirectStandardOutput $log -RedirectStandardError "$log.err" -Wait -PassThru -NoNewWindow
    if ($proc.ExitCode -eq 0) { Write-Host "[$( Get-Date -Format HH:mm:ss)] $($names[$i]) done" }
    else { Write-Host "$($names[$i]) failed"; exit 1 }
}
Write-Host "Pipeline Done. Logs: $LOG_DIR"
