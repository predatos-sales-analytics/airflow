@echo off
REM Script para ejecutar flows de Prefect

setlocal

REM Cambiar al directorio raiz del proyecto
cd /d "%~dp0..\.."

REM Verificar argumento
if "%~1"=="" (
    echo [ERROR] Debes especificar el nombre del flow
    echo.
    echo Uso: %~nx0 ^<flow_name^> [options]
    echo.
    echo Flows disponibles:
    echo   data_loading      - Cargar datos CSV a PostgreSQL
    echo   data_monitor      - Monitorear nuevos datos en la base de datos
    echo   master            - Ejecutar todos los pipelines
    echo   executive_summary - Pipeline de resumen ejecutivo
    echo   analytics         - Pipeline de analisis temporal
    echo   clustering        - Pipeline de clustering de clientes
    echo   recommendations   - Pipeline de recomendaciones
    echo   output_sync       - Sincronizar outputs al frontend
    echo.
    echo Ejemplos:
    echo   %~nx0 data_loading
    echo   %~nx0 master
    echo   %~nx0 clustering --n-clusters 5
    echo.
    exit /b 1
)

set FLOW_NAME=%~1
shift

REM Mapear nombre de flow a modulo Python
if "%FLOW_NAME%"=="data_loading" (
    set FLOW_MODULE=flows.data_loading_flow
) else if "%FLOW_NAME%"=="data_monitor" (
    set FLOW_MODULE=flows.data_monitor_flow
) else if "%FLOW_NAME%"=="master" (
    set FLOW_MODULE=flows.master_flow
) else if "%FLOW_NAME%"=="executive_summary" (
    set FLOW_MODULE=flows.executive_summary_flow
) else if "%FLOW_NAME%"=="analytics" (
    set FLOW_MODULE=flows.analytics_flow
) else if "%FLOW_NAME%"=="clustering" (
    set FLOW_MODULE=flows.clustering_flow
) else if "%FLOW_NAME%"=="recommendations" (
    set FLOW_MODULE=flows.recommendations_flow
) else if "%FLOW_NAME%"=="output_sync" (
    set FLOW_MODULE=flows.output_sync_flow
) else (
    echo [ERROR] Flow desconocido: %FLOW_NAME%
    exit /b 1
)

REM Verificar que el servicio este corriendo
docker compose ps | findstr /C:"prefect-worker" | findstr /C:"Up" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] El servicio prefect-worker no esta corriendo
    echo Ejecuta: docker compose up -d
    exit /b 1
)

echo [INFO] Ejecutando flow: %FLOW_NAME%
echo.

REM Ejecutar el flow en el worker
docker compose exec -T prefect-worker python -m %FLOW_MODULE% %*

echo.
echo [OK] Flow ejecutado. Revisa la UI de Prefect para mas detalles: http://localhost:4200

endlocal

