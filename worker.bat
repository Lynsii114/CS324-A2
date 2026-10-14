@echo off
setlocal

set "JAR=%~dp0worker\target\worker.jar"

if not exist "%JAR%" (
  echo Missing worker\target\worker.jar. Build first with:
  echo .\.tools\apache-maven-3.10.0\bin\mvn.cmd package
  exit /b 1
)

if "%~1"=="" (
  java -jar "%JAR%"
  exit /b %ERRORLEVEL%
)

if /i "%~1"=="auto" (
  java -jar "%JAR%" %*
  exit /b %ERRORLEVEL%
)

set "ID=%~1"
set /a PORT=5000+ID

if %ID% LSS 1 (
  echo Worker id must be a positive integer.
  exit /b 1
)

set "BOOTSTRAP_HOST=%~2"
set "BOOTSTRAP_PORT=%~3"
set "WORKER_HOST=%~4"

if "%BOOTSTRAP_HOST%"=="" set "BOOTSTRAP_HOST=localhost"
if "%BOOTSTRAP_PORT%"=="" set "BOOTSTRAP_PORT=1099"
if "%WORKER_HOST%"=="" set "WORKER_HOST=localhost"

java -jar "%JAR%" %ID% %PORT% %BOOTSTRAP_HOST% %BOOTSTRAP_PORT% %WORKER_HOST%
