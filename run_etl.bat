@echo off
rem Run the weather ETL and append output to a log file.
rem Schedule this file with Windows Task Scheduler (daily).
cd /d "%~dp0"
if not exist logs mkdir logs
python etl.py >> logs\etl_run.log 2>&1
