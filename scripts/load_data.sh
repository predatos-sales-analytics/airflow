#!/usr/bin/env bash
set -euo pipefail

# Si se ejecuta desde dentro del contenedor, usar postgres como host
if [ -f /.dockerenv ]; then
    PGHOST="${PGHOST:-postgres}"
else
    PGHOST="${PGHOST:-localhost}"
fi

DATA_DIR="${1:-data}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-sales}"
PGUSER="${PGUSER:-sales}"
PGPASSWORD="${PGPASSWORD:-sales}"

export PGPASSWORD

psql_cmd() {
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${PGDATABASE}" "$@"
}

echo "▶️ Creando tablas si no existen..."
psql_cmd <<'EOSQL'
CREATE TABLE IF NOT EXISTS categories (
    category_id INT PRIMARY KEY,
    category_name TEXT
);

CREATE TABLE IF NOT EXISTS product_categories (
    product_id INT,
    category_id INT
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_date DATE,
    store_id INT,
    customer_id INT,
    products TEXT
);
EOSQL

echo "🧹 Limpiando tablas..."
psql_cmd -c "TRUNCATE TABLE categories;"
psql_cmd -c "TRUNCATE TABLE product_categories;"
psql_cmd -c "TRUNCATE TABLE transactions;"

echo "📥 Cargando categorías..."
psql_cmd -c "\copy categories(category_id, category_name) FROM '${DATA_DIR}/products/Categories.csv' WITH (FORMAT csv, DELIMITER '|');"

echo "📥 Cargando productos-categorías..."
psql_cmd -c "\copy product_categories(product_id, category_id) FROM '${DATA_DIR}/products/ProductCategory.csv' WITH (FORMAT csv, DELIMITER '|', HEADER true);"

echo "📥 Cargando transacciones..."
for file in "${DATA_DIR}"/transactions/*_Tran.csv; do
    echo "   • ${file}"
    psql_cmd -c "\copy transactions(transaction_date, store_id, customer_id, products) FROM '${file}' WITH (FORMAT csv, DELIMITER '|');"
done

echo "✅ Carga completada."

