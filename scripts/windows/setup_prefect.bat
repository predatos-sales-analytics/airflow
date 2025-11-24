@echo off
REM Script para configurar Prefect Server y registrar flows

setlocal enabledelayedexpansion

echo ================================================================================
echo CONFIGURACION DE PREFECT
echo ================================================================================
echo.

REM Cambiar al directorio raiz del proyecto
cd /d "%~dp0..\.."

REM Verificar que docker este disponible
where docker >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] docker no se encuentra en el PATH
    echo Asegurate de tener Docker Desktop instalado y en ejecucion
    exit /b 1
)

REM Verificar que los servicios esten corriendo
echo [INFO] Verificando servicios de Docker...
docker compose ps | findstr /C:"prefect-server" | findstr /C:"Up" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] El servicio prefect-server no esta corriendo
    echo Ejecuta: docker compose up -d
    exit /b 1
)

echo [OK] Servicios de Docker activos

REM Esperar a que Prefect Server este listo
echo [INFO] Esperando a que Prefect Server este listo (esto puede tomar 20-30 segundos)...
set RETRY=0
:wait_loop
if !RETRY! geq 30 (
    echo [ERROR] Timeout esperando a Prefect Server
    echo [INFO] Puedes intentar acceder manualmente a http://localhost:4200
    exit /b 1
)

REM Usar PowerShell para hacer una peticion HTTP
powershell -Command "try { $response = Invoke-WebRequest -Uri 'http://localhost:4200/api/health' -TimeoutSec 2 -UseBasicParsing; exit 0 } catch { exit 1 }" >nul 2>&1
if %ERRORLEVEL% equ 0 (
    echo [OK] Prefect Server esta listo
    goto :server_ready
)

set /a RETRY+=1
echo [INFO] Intento !RETRY!/30...
timeout /t 2 /nobreak >nul
goto :wait_loop

:server_ready

REM Crear work pool si no existe
echo [INFO] Creando work pool 'default-pool'...
docker compose exec -T prefect-worker prefect work-pool create default-pool --type process 2>nul
if %ERRORLEVEL% equ 0 (
    echo [OK] Work pool creado exitosamente
) else (
    echo [INFO] Work pool ya existe o hubo un error menor (puedes ignorar esto)
)

REM Crear deployment del monitor de datos
echo.
echo [INFO] Configurando deployment automatico del monitor de datos...
docker compose exec -T prefect-worker python src/deployments/monitor_deployment.py
if %ERRORLEVEL% equ 0 (
    echo [OK] Deployment del monitor configurado (ejecutara cada 2 minutos)
) else (
    echo [WARNING] No se pudo crear el deployment automatico
    echo [INFO] Puedes ejecutar el monitor manualmente: scripts\windows\monitor_data.bat
)

REM Informacion de acceso
echo.
echo ================================================================================
echo CONFIGURACION COMPLETADA
echo ================================================================================
echo.
echo Prefect esta configurado y listo para usar
echo.
echo Acceso a la interfaz web:
echo   URL: http://localhost:4200
echo.
echo Monitor de datos:
echo   - Configurado para ejecutarse automaticamente cada 2 minutos
echo   - Gestiona desde la UI: http://localhost:4200/deployments
echo   - Ejecucion manual: scripts\windows\monitor_data.bat
echo.
echo Para ejecutar flows, usa los scripts en scripts\windows\:
echo   scripts\windows\run_prefect_flow.bat data_loading
echo   scripts\windows\run_prefect_flow.bat master
echo   scripts\windows\run_prefect_flow.bat executive_summary
echo   scripts\windows\run_prefect_flow.bat analytics
echo   scripts\windows\run_prefect_flow.bat clustering
echo   scripts\windows\run_prefect_flow.bat recommendations
echo   scripts\windows\run_prefect_flow.bat output_sync
echo.

endlocal

