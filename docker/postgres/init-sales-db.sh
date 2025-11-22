#!/usr/bin/env bash
set -euo pipefail

# Crear el rol/usuario si no existe
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" <<-EOSQL
    DO
    \$\$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles WHERE rolname = 'sales'
        ) THEN
            CREATE ROLE sales LOGIN PASSWORD 'sales';
        END IF;
    END
    \$\$;
    -- Otorgar permisos necesarios para COPY
    ALTER ROLE sales WITH SUPERUSER;
EOSQL

# Crear la base de datos si no existe (No puede estar dentro de la sentencia DO)
DB_EXISTS=$(psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" -tAc "SELECT 1 FROM pg_database WHERE datname='sales'")
if [ -z "$DB_EXISTS" ]; then
    psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" <<-EOSQL
        CREATE DATABASE sales OWNER sales;
EOSQL
fi
