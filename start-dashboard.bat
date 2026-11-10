@echo off
title MineRelay - Distributed Minecraft Server Dashboard
cd /d "%~dp0"
echo Checking Python libraries...
python -m pip install -r requirements.txt --quiet --disable-pip-version-check 2>nul
echo Starting MineRelay Dashboard...
python src/server.py
pause
