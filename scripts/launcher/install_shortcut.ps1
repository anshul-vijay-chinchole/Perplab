# Create the "PerpLab" desktop shortcut.
#
# Points at PerpLab.vbs (the hidden launcher) via wscript, with the generated icon.
# Run once:  powershell -ExecutionPolicy Bypass -File scripts\launcher\install_shortcut.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$vbs = Join-Path $root 'PerpLab.vbs'
$icon = Join-Path $root 'assets\perplab.ico'
$desktop = [Environment]::GetFolderPath('Desktop')
$lnkPath = Join-Path $desktop 'PerpLab.lnk'

if (-not (Test-Path $vbs)) { throw "launcher not found: $vbs" }
if (-not (Test-Path $icon)) {
    Write-Host 'Icon missing — generating it first…'
    & (Join-Path $root '.venv\Scripts\python.exe') (Join-Path $PSScriptRoot 'make_icon.py')
}

$shell = New-Object -ComObject WScript.Shell
$lnk = $shell.CreateShortcut($lnkPath)
$lnk.TargetPath = "$env:SystemRoot\System32\wscript.exe"
$lnk.Arguments = """$vbs"""
$lnk.WorkingDirectory = $root
$lnk.IconLocation = "$icon,0"
$lnk.Description = 'PerpLab — algorithmic trading platform'
$lnk.Save()

Write-Host "Created $lnkPath"
Write-Host 'Double-click it to start the platform — no terminal window will appear.'
