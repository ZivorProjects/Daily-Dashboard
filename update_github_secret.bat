@echo off
title Update GitHub Secret - Zivor Dashboard
color 0A
echo.
echo ============================================================
echo   Zivor Dashboard - Update GitHub Secret (CONFIG_JSON_B64)
echo ============================================================
echo.

:: Check config.json exists
if not exist "config.json" (
    echo [ERROR] config.json not found in this folder.
    echo         Make sure you run this from the Zivor Dashboard folder.
    pause
    exit /b 1
)

:: Encode config.json to base64 and copy to clipboard
echo [1/3] Encoding config.json to base64...
powershell -NoProfile -Command "$bytes = [System.IO.File]::ReadAllBytes('config.json'); $b64 = [System.Convert]::ToBase64String($bytes); Set-Clipboard -Value $b64; Write-Output \"    Done. $($b64.Length) characters copied to clipboard.\""

echo.
echo [2/3] Opening GitHub Secrets page in your browser...
start https://github.com/ZivorProjects/Daily-Dashboard/settings/secrets/actions/CONFIG_JSON_B64

echo.
echo [3/3] Follow these steps in the browser:
echo.
echo   1. Click "Update secret"
echo   2. Click inside the "Secret" box
echo   3. Press Ctrl+A to select all (clear the old value)
echo   4. Press Ctrl+V to paste the new value
echo   5. Click "Update secret"
echo.
echo ============================================================
echo   The base64 value is already in your clipboard. 
echo   Just Ctrl+A then Ctrl+V in the Secret box.
echo ============================================================
echo.
pause
