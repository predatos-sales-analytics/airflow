@echo off
REM Script para monitorear nuevos datos en PostgreSQL
REM Este script ejecuta el monitor una vez y reporta si hay datos nuevos

setlocal

REM Cambiar al directorio raiz del proyecto
cd /d "%~dp0..\.."

REM Verificar que el servicio este corriendo
docker compose ps | findstr /C:"prefect-worker" | findstr /C:"Up" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] El servicio prefect-worker no esta corriendo
    echo Ejecuta: docker compose up -d
    exit /b 1
)

echo ========================================
echo      MONITOR DE NUEVOS DATOS
echo ========================================
echo.

REM Ejecutar el flow de monitoreo
docker compose exec -T prefect-worker python -m flows.data_monitor_flow

echo.
echo ========================================
echo Para ejecutar el master flow manualmente:
echo   scripts\windows\run_prefect_flow.bat master
echo.
echo Para ver detalles en la UI de Prefect:
echo   http://localhost:4200
echo ========================================

endlocal

