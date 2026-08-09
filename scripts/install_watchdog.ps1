<#
.SYNOPSIS
    Register a Windows Scheduled Task that keeps the PerpLab collector running.

.DESCRIPTION
    The in-process supervisor (perplab/data/supervisor.py) catches unhandled exceptions
    and restarts the collector loop. It cannot catch interpreter death, an OOM kill, or a
    machine reboot -- and those are exactly the failures that would end an unattended
    72-hour run with no data and no alert.

    This task covers that outer layer. It launches the collector at startup and re-checks
    every 5 minutes, so any exit is recovered within 5 minutes.

    Restarts are not silent: the collector detects an unclean shutdown from the state file
    left behind and writes a RESTART record into the collectorEvents stream with the
    measured downtime. Gaps in the data are therefore always attributable.

.NOTES
    REVIEW THIS BEFORE RUNNING. It creates a task that starts a process automatically at
    every boot. Remove it with:
        schtasks /Delete /TN "PerpLab Collector" /F

.EXAMPLE
    .\scripts\install_watchdog.ps1 -WhatIf
    .\scripts\install_watchdog.ps1
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskName = "PerpLab Collector",
    [string]$Symbol = "BTCUSDT",
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"

$pythonExe = Join-Path $ProjectRoot ".venv\Scripts\pythonw.exe"
if (-not (Test-Path $pythonExe)) {
    # pythonw.exe runs without a console window, which suits a background task. Fall back
    # to python.exe rather than failing if the venv layout differs.
    $pythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
}
if (-not (Test-Path $pythonExe)) {
    throw "No Python found in $ProjectRoot\.venv\Scripts. Create the venv first."
}

$logDir = Join-Path $ProjectRoot "userdata\logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Write-Host "Task name   : $TaskName"
Write-Host "Python      : $pythonExe"
Write-Host "Project root: $ProjectRoot"
Write-Host "Symbol      : $Symbol"
Write-Host ""

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Write-Warning "A task named '$TaskName' already exists and will be replaced."
}

# --supervise gives the inner catch-all as well, so a crash is usually recovered in
# seconds; the 5-minute task trigger is only the backstop for process-level death.
$action = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "-m perplab collect --symbol $Symbol --supervise" `
    -WorkingDirectory $ProjectRoot

$atStartup = New-ScheduledTaskTrigger -AtStartup

$every5Min = New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Minutes 5) `
    -RepetitionDuration ([TimeSpan]::MaxValue)

# IgnoreNew means the 5-minute trigger is a no-op while the collector is already running,
# so this re-checks liveness rather than spawning duplicate collectors.
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

if ($PSCmdlet.ShouldProcess($TaskName, "Register scheduled task")) {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger @($atStartup, $every5Min) `
        -Settings $settings `
        -Description "Keeps the PerpLab market data collector running. Depth history only accumulates forward, so downtime is permanent data loss." `
        -Force | Out-Null

    Write-Host "Registered. Start it now with:" -ForegroundColor Green
    Write-Host "    Start-ScheduledTask -TaskName '$TaskName'"
    Write-Host ""
    Write-Host "Check status:  Get-ScheduledTask -TaskName '$TaskName' | Get-ScheduledTaskInfo"
    Write-Host "Remove:        schtasks /Delete /TN '$TaskName' /F"
    Write-Host ""
    Write-Warning "Run 'python -m perplab preflight' first -- a machine that sleeps will still stop the collector, and this task cannot prevent that."
}
