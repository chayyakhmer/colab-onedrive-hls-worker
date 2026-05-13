@echo off
setlocal EnableExtensions EnableDelayedExpansion

echo ============================================================
echo  Push Colab OneDrive HLS Worker UI to GitHub
echo ============================================================
echo.

set /p REPO_DIR=Enter local repo folder path [C:\Users\Kim\Desktop\Current_Project\colab-onedrive-hls-worker]: 
if "%REPO_DIR%"=="" set REPO_DIR=C:\Users\Kim\Desktop\Current_Project\colab-onedrive-hls-worker

set /p GIT_URL=Enter GitHub repo URL [https://github.com/chayyakhmer/colab-onedrive-hls-worker.git]: 
if "%GIT_URL%"=="" set GIT_URL=https://github.com/chayyakhmer/colab-onedrive-hls-worker.git

if not exist "%REPO_DIR%\.git" (
  echo Cloning repo...
  git clone "%GIT_URL%" "%REPO_DIR%"
  if errorlevel 1 (
    echo Clone failed.
    pause
    exit /b 1
  )
) else (
  echo Repo exists. Pulling latest...
  cd /d "%REPO_DIR%"
  git pull
  cd /d "%~dp0"
)

echo Copying updated files...
copy /Y "%~dp0colab_worker.py" "%REPO_DIR%\colab_worker.py"
copy /Y "%~dp0requirements.txt" "%REPO_DIR%\requirements.txt"
copy /Y "%~dp0README.md" "%REPO_DIR%\README.md"
copy /Y "%~dp0colab_quickstart.py" "%REPO_DIR%\colab_quickstart.py"

cd /d "%REPO_DIR%"
git add colab_worker.py requirements.txt README.md colab_quickstart.py

git diff --cached --quiet
if not errorlevel 1 (
  echo No changes to commit.
  pause
  exit /b 0
)

git commit -m "Add Colab OneDrive HLS web UI"
git push

echo Done.
pause
