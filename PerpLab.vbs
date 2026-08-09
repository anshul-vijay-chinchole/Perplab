' PerpLab — hidden launcher.
'
' Runs the PowerShell launcher with no console window, so double-clicking the desktop
' shortcut opens the platform and nothing else. All logic lives in
' scripts\launcher\start-platform.ps1; this file exists only to hide the window, which
' a .lnk cannot do for a console program on its own.

Dim shell, root
Set shell = CreateObject("WScript.Shell")
root = Left(WScript.ScriptFullName, InStrRev(WScript.ScriptFullName, "\"))
shell.Run "powershell.exe -NoProfile -ExecutionPolicy Bypass -File """ & root & "scripts\launcher\start-platform.ps1""", 0, False
