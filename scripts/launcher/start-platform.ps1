# PerpLab one-click launcher.
#
# One process: `perplab serve` hosts the API *and* the built web UI on 127.0.0.1:8756
# (frontend/dist is served by the FastAPI app). So "launch the platform" is: make sure
# that process is up, wait for /api/health, then open an app-mode browser window.
#
# Idempotent on purpose — double-clicking the shortcut twice must not start a second
# server. If the port already answers health, this script only opens the window.
#
# The collector is a *separate* python process and this script never touches it.

$ErrorActionPreference = 'SilentlyContinue'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # repo root (scripts\launcher\..\..)
$python = Join-Path $root '.venv\Scripts\python.exe'
$url = 'http://127.0.0.1:8756'
$logDir = Join-Path $root 'userdata\logs'
$log = Join-Path $logDir 'serve.log'

function Test-Health {
    try {
        $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri "$url/api/health"
        return $response.StatusCode -eq 200
    } catch {
        return $false
    }
}

if (-not (Test-Health)) {
    if (-not (Test-Path $python)) {
        # No venv — nothing sensible to do silently. Surface it the one way a hidden
        # window can.
        Add-Type -AssemblyName PresentationFramework
        [System.Windows.MessageBox]::Show(
            "PerpLab's Python environment was not found at`n$python`n`nRun the setup steps in README.md first.",
            'PerpLab') | Out-Null
        exit 1
    }
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force $logDir | Out-Null }

    # Start the server detached and windowless. Output goes to a log file, because a
    # hidden window is not a licence to throw the logs away.
    Start-Process -FilePath $python `
        -ArgumentList '-m', 'perplab', 'serve' `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $log `
        -RedirectStandardError ($log -replace '\.log$', '.err.log')

    # Wait for it to answer, up to 30 s. The UI opening before the API is up would greet
    # the user with "Cannot reach the PerpLab API" on every cold start.
    $up = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        if (Test-Health) { $up = $true; break }
    }
    if (-not $up) {
        Add-Type -AssemblyName PresentationFramework
        [System.Windows.MessageBox]::Show(
            "The PerpLab server did not come up within 30 seconds.`nCheck $log for the reason.",
            'PerpLab') | Out-Null
        exit 1
    }
}

# App-mode window: no URL bar, its own taskbar entry — the platform as a product, not a
# tab. Edge ships with Windows 11; fall back to the default browser if it is missing.
$edge = Get-Command 'msedge.exe' -ErrorAction SilentlyContinue
if ($null -eq $edge) {
    $edgePath = "$env:ProgramFiles (x86)\Microsoft\Edge\Application\msedge.exe"
    if (Test-Path $edgePath) { $edge = @{ Source = $edgePath } }
}
if ($null -ne $edge) {
    Start-Process $edge.Source "--app=$url"
} else {
    Start-Process $url
}
