@echo off
setlocal enabledelayedexpansion

set "DATA_DIR=%~1"
if "%DATA_DIR%"=="" set "DATA_DIR=data"

REM Detectar si estamos dentro de Docker (verificar variable de entorno)
if "%PGHOST%"=="" (
    if defined DOCKER_CONTAINER (
        set "PGHOST=postgres"
    ) else (
        set "PGHOST=localhost"
    )
)
if "%PGPORT%"=="" set "PGPORT=5432"
if "%PGDATABASE%"=="" set "PGDATABASE=sales"
if "%PGUSER%"=="" set "PGUSER=sales"
if "%PGPASSWORD%"=="" set "PGPASSWORD=sales"

set "PSQL_CMD=psql -h %PGHOST% -p %PGPORT% -U %PGUSER% -d %PGDATABASE%"

echo Creando tablas (si no existen)...
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS categories (category_id INT PRIMARY KEY, category_name TEXT);"
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS product_categories (product_id INT, category_id INT);"
%PSQL_CMD% -c "CREATE TABLE IF NOT EXISTS transactions (transaction_date DATE, store_id INT, customer_id INT, products TEXT);"

echo Limpiando tablas...
%PSQL_CMD% -c "TRUNCATE TABLE categories;"
%PSQL_CMD% -c "TRUNCATE TABLE product_categories;"
%PSQL_CMD% -c "TRUNCATE TABLE transactions;"

echo Cargando categorias...
%PSQL_CMD% -c "\copy categories(category_id, category_name) FROM '%DATA_DIR%\products\Categories.csv' WITH (FORMAT csv, DELIMITER '|');"

echo Cargando productos-categorias...
%PSQL_CMD% -c "\copy product_categories(product_id, category_id) FROM '%DATA_DIR%\products\ProductCategory.csv' WITH (FORMAT csv, DELIMITER '|', HEADER true);"

echo Cargando transacciones...
for %%F in ("%DATA_DIR%\transactions\*_Tran.csv") do (
    echo   - %%~fF
    %PSQL_CMD% -c "\copy transactions(transaction_date, store_id, customer_id, products) FROM '%%~fF' WITH (FORMAT csv, DELIMITER '|');"
)

echo Carga completada.
endlocal

