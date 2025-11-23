@echo off
setlocal enabledelayedexpansion

REM Cambiar al directorio raiz del proyecto
cd /d "%~dp0..\.."

if "%1"=="" (
    echo.
    echo ================================================================================
    echo EJECUTAR PIPELINES DE PROCESAMIENTO
    echo ================================================================================
    echo.
    echo Uso: ./scripts/windows/run_pipeline_docker.bat ^<pipeline_name^> [opciones]
    echo.
    echo Pipelines disponibles:
    echo   - executive_summary  - Resumen ejecutivo ^(diario^)
    echo   - analytics          - Analisis analitico ^(semanal^)
    echo   - clustering         - Clustering de clientes ^(mensual^)
    echo   - recommendations    - Recomendaciones de productos ^(mensual^)
    echo   - all                - Ejecutar todos los pipelines
    echo.
    echo Ejemplos:
    echo   ./scripts/windows/run_pipeline_docker.bat executive_summary
    echo   ./scripts/windows/run_pipeline_docker.bat clustering --n-clusters 5
    echo   ./scripts/windows/run_pipeline_docker.bat recommendations --min-support 0.02
    echo.
    exit /b 1
)

set "PIPELINE_NAME=%1"

REM Verificar que docker compose este disponible
where docker >nul 2>&1
if errorlevel 1 (
    echo [ERROR] docker no se encuentra en el PATH
    echo [INFO]  Asegurate de tener Docker Desktop instalado y en ejecucion
    exit /b 1
)

REM Verificar que el contenedor spark-client este corriendo
docker compose ps spark-client | findstr "Up" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] El contenedor spark-client no esta corriendo
    echo [INFO]  Ejecuta: docker compose up -d
    exit /b 1
)

echo.
echo ================================================================================
echo [INFO] Ejecutando pipeline: %PIPELINE_NAME%
echo ================================================================================
echo.

REM Ejecutar el pipeline con todos los argumentos
docker compose exec spark-client python3 /opt/spark/work-dir/src/run_pipeline.py %*

if errorlevel 1 (
    echo.
    echo ================================================================================
    echo [ERROR] Pipeline '%PIPELINE_NAME%' fallo
    echo ================================================================================
    echo.
    exit /b 1
) else (
    echo.
    echo ================================================================================
    echo [OK] Pipeline '%PIPELINE_NAME%' completado exitosamente
    echo ================================================================================
    echo.
    exit /b 0
)

endlocal
