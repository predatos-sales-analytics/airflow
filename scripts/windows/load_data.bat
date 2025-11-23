@echo off
setlocal enabledelayedexpansion

REM Cambiar al directorio raiz del proyecto
cd /d "%~dp0..\.."

REM El directorio data esta montado en /data dentro del contenedor
set "DOCKER_DATA_DIR=/data"

REM Archivo de transaccion especifico (opcional)
set "TRANSACTION_FILE=%~1"

echo.
echo ================================================================================
echo SCRIPT DE CARGA DE DATOS A POSTGRESQL
echo ================================================================================
echo.

REM Verificar que docker compose este disponible
where docker >nul 2>&1
if errorlevel 1 (
    echo [ERROR] docker no se encuentra en el PATH
    echo [INFO]  Asegurate de tener Docker Desktop instalado y en ejecucion
    exit /b 1
)

REM Verificar que el contenedor de postgres este corriendo
docker compose ps postgres | findstr "Up" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] El contenedor de postgres no esta corriendo
    echo [INFO]  Ejecuta: docker compose up -d postgres
    exit /b 1
)

set "PSQL_CMD=docker compose exec -T postgres psql -U sales -d sales"

echo --------------------------------------------------------------------------------
echo PREPARACION DE BASE DE DATOS
echo --------------------------------------------------------------------------------
echo.

echo [1/3] Creando tablas si no existen...
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS categories (category_id INT PRIMARY KEY, category_name TEXT);" >nul 2>&1
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS product_categories (product_id INT, category_id INT);" >nul 2>&1
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS transactions (transaction_date DATE, store_id INT, customer_id INT, products TEXT);" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] No se pudieron crear las tablas
    exit /b 1
)
echo [OK] Tablas creadas/verificadas

echo [2/3] Limpiando datos existentes...
%PSQL_CMD% -c "TRUNCATE TABLE categories;" >nul 2>&1
%PSQL_CMD% -c "TRUNCATE TABLE product_categories;" >nul 2>&1
%PSQL_CMD% -c "TRUNCATE TABLE transactions;" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] No se pudieron limpiar las tablas
    exit /b 1
)
echo [OK] Tablas limpiadas

echo.
echo --------------------------------------------------------------------------------
echo CARGA DE DATOS DE REFERENCIA
echo --------------------------------------------------------------------------------
echo.

echo [1/2] Cargando categorias...
%PSQL_CMD% -c "\copy categories(category_id, category_name) FROM '%DOCKER_DATA_DIR%/products/Categories.csv' WITH (FORMAT csv, DELIMITER '|');" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] No se pudo cargar el archivo de categorias
    exit /b 1
)
for /f "tokens=*" %%C in ('%PSQL_CMD% -t -c "SELECT COUNT(*) FROM categories;"') do set COUNT_CATEGORIES=%%C
set COUNT_CATEGORIES=!COUNT_CATEGORIES: =!
echo [OK] Categorias cargadas: !COUNT_CATEGORIES! registros

echo [2/2] Cargando productos-categorias...
%PSQL_CMD% -c "\copy product_categories(product_id, category_id) FROM '%DOCKER_DATA_DIR%/products/ProductCategory.csv' WITH (FORMAT csv, DELIMITER '|', HEADER true);" >nul 2>&1
if errorlevel 1 (
    echo [ERROR] No se pudo cargar el archivo de productos-categorias
    exit /b 1
)
for /f "tokens=*" %%P in ('%PSQL_CMD% -t -c "SELECT COUNT(*) FROM product_categories;"') do set COUNT_PRODUCTS=%%P
set COUNT_PRODUCTS=!COUNT_PRODUCTS: =!
echo [OK] Productos-categorias cargadas: !COUNT_PRODUCTS! registros

echo.
echo --------------------------------------------------------------------------------
echo CARGA DE TRANSACCIONES
echo --------------------------------------------------------------------------------
echo.

if not "!TRANSACTION_FILE!"=="" (
    REM Cargar un archivo especifico
    echo [INFO] Cargando archivo especifico: !TRANSACTION_FILE!
    
    REM Extraer solo el nombre del archivo
    for %%F in ("!TRANSACTION_FILE!") do set "FILE_NAME=%%~nxF"
    
    REM Verificar que sea un archivo de transacciones
    echo !FILE_NAME! | findstr /R "_Tran.csv$" >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] El archivo debe ser un CSV de transacciones ^(formato: *_Tran.csv^)
        exit /b 1
    )
    
    REM Construir la ruta dentro del contenedor
    set "FILE_PATH=%DOCKER_DATA_DIR%/transactions/!FILE_NAME!"
    
    echo [INFO] Ruta del archivo en el contenedor: !FILE_PATH!
    echo [INFO] Cargando archivo...
    
    %PSQL_CMD% -c "COPY transactions(transaction_date, store_id, customer_id, products) FROM '!FILE_PATH!' WITH (FORMAT csv, DELIMITER '|');" >nul 2>&1
    if errorlevel 1 (
        echo [ERROR] No se pudo cargar el archivo !FILE_NAME!
        exit /b 1
    )
    
    for /f "tokens=*" %%T in ('%PSQL_CMD% -t -c "SELECT COUNT(*) FROM transactions;"') do set COUNT_TRANSACTIONS=%%T
    set COUNT_TRANSACTIONS=!COUNT_TRANSACTIONS: =!
    echo [OK] Archivo !FILE_NAME! cargado. Total de transacciones: !COUNT_TRANSACTIONS!
) else (
    REM Cargar todas las transacciones
    echo [INFO] Cargando todas las transacciones desde %DOCKER_DATA_DIR%/transactions/...
    echo.
    
    set LOADED_FILES=0
    set FAILED_FILES=0
    
    REM Lista de archivos de transacciones conocidos (se pueden agregar mas)
    set "FILES=102_Tran.csv 103_Tran.csv 107_Tran.csv 110_Tran.csv"
    
    for %%F in (!FILES!) do (
        set "FILE_NAME=%%F"
        set "FILE_PATH=%DOCKER_DATA_DIR%/transactions/!FILE_NAME!"
        
        echo [INFO]   -> Cargando: !FILE_NAME!...
        
        %PSQL_CMD% -c "COPY transactions(transaction_date, store_id, customer_id, products) FROM '!FILE_PATH!' WITH (FORMAT csv, DELIMITER '|');" >nul 2>&1
        if not errorlevel 1 (
            echo [OK]      !FILE_NAME! cargado exitosamente
            set /a LOADED_FILES+=1
        ) else (
            echo [WARNING] Error al cargar !FILE_NAME! ^(archivo puede no existir^)
            set /a FAILED_FILES+=1
        )
    )
    
    if !LOADED_FILES! equ 0 (
        echo [ERROR] No se pudieron cargar archivos de transacciones
        echo [INFO]  Verifica que los archivos existan en el directorio data/transactions/
        exit /b 1
    )
    
    for /f "tokens=*" %%T in ('%PSQL_CMD% -t -c "SELECT COUNT(*) FROM transactions;"') do set COUNT_TRANSACTIONS=%%T
    set COUNT_TRANSACTIONS=!COUNT_TRANSACTIONS: =!
    echo.
    if !FAILED_FILES! gtr 0 (
        echo [WARNING] Algunos archivos no se pudieron cargar: !FAILED_FILES! archivo^(s^)
    )
    echo [OK] Transacciones cargadas: !LOADED_FILES! archivo^(s^), !COUNT_TRANSACTIONS! registro^(s^) totales
)

echo.
echo --------------------------------------------------------------------------------
echo RESUMEN FINAL
echo --------------------------------------------------------------------------------
echo.

for /f "tokens=*" %%C in ('%PSQL_CMD% -t -c "SELECT COUNT(*) FROM categories;"') do set TOTAL_CATEGORIES=%%C
for /f "tokens=*" %%P in ('%PSQL_CMD% -t -c "SELECT COUNT(DISTINCT product_id) FROM product_categories;"') do set TOTAL_PRODUCTS=%%P
for /f "tokens=*" %%T in ('%PSQL_CMD% -t -c "SELECT COUNT(*) FROM transactions;"') do set TOTAL_TRANSACTIONS=%%T

set TOTAL_CATEGORIES=!TOTAL_CATEGORIES: =!
set TOTAL_PRODUCTS=!TOTAL_PRODUCTS: =!
set TOTAL_TRANSACTIONS=!TOTAL_TRANSACTIONS: =!

echo Categorias:        !TOTAL_CATEGORIES!
echo Productos unicos:  !TOTAL_PRODUCTS!
echo Transacciones:     !TOTAL_TRANSACTIONS!
echo.
echo ================================================================================
echo CARGA DE DATOS COMPLETADA
echo ================================================================================
echo.

endlocal
