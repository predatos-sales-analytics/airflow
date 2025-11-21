@echo off
setlocal enabledelayedexpansion

set "DATA_DIR=%~1"
if "%DATA_DIR%"=="" set "DATA_DIR=data"

REM Convertir ruta de Windows a ruta dentro del contenedor Docker
REM El directorio data está montado en /data dentro del contenedor
set "DOCKER_DATA_DIR=/data"

REM Verificar que docker-compose esté disponible
where docker-compose >nul 2>&1
if errorlevel 1 (
    echo Error: docker-compose no se encuentra en el PATH
    echo Asegúrate de tener Docker Desktop instalado y en ejecución
    exit /b 1
)

REM Verificar que el contenedor de postgres esté corriendo
docker-compose ps postgres | findstr "Up" >nul 2>&1
if errorlevel 1 (
    echo Error: El contenedor de postgres no está corriendo
    echo Ejecuta: docker-compose up -d postgres
    exit /b 1
)

set "PSQL_CMD=docker-compose exec -T postgres psql -U sales -d sales"

echo Creando tablas (si no existen)...
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS categories (category_id INT PRIMARY KEY, category_name TEXT);"
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS product_categories (product_id INT, category_id INT);"
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS transactions (transaction_date DATE, store_id INT, customer_id INT, products TEXT);"

echo Limpiando tablas...
%PSQL_CMD% -c "TRUNCATE TABLE categories;"
%PSQL_CMD% -c "TRUNCATE TABLE product_categories;"
%PSQL_CMD% -c "TRUNCATE TABLE transactions;"

echo Cargando categorias...
%PSQL_CMD% -c "\copy categories(category_id, category_name) FROM '%DOCKER_DATA_DIR%/products/Categories.csv' WITH (FORMAT csv, DELIMITER '|');"

echo Cargando productos-categorias...
%PSQL_CMD% -c "\copy product_categories(product_id, category_id) FROM '%DOCKER_DATA_DIR%/products/ProductCategory.csv' WITH (FORMAT csv, DELIMITER '|', HEADER true);"

echo Cargando transacciones...
set "TRANSACTIONS_DIR=%DATA_DIR%\transactions"
if not exist "%TRANSACTIONS_DIR%" (
    echo Error: El directorio de transacciones no existe: %TRANSACTIONS_DIR%
    exit /b 1
)
echo Buscando archivos en: %TRANSACTIONS_DIR%
pushd "%TRANSACTIONS_DIR%"
for %%F in (*_Tran.csv) do (
    echo Archivo encontrado: %%F
    REM Obtener solo el nombre del archivo
    set "FILENAME=%%~nxF"
    REM Convertir a ruta dentro del contenedor
    set "DOCKER_FILE=%DOCKER_DATA_DIR%/transactions/!FILENAME!"
    echo   Cargando: !FILENAME!
    docker-compose exec -T postgres psql -U sales -d sales -c "COPY transactions(transaction_date, store_id, customer_id, products) FROM '!DOCKER_FILE!' WITH (FORMAT csv, DELIMITER '|');"
)
popd

echo Carga completada.
endlocal

