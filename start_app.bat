@echo off

REM run compose
docker-compose -p cez_ftp_data up -d --build

REM keep the window alive to see logs
pause
