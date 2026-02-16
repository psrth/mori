-- DuckDB E2E Test Seed Schema
-- 6 tables, diverse PK types
-- Sequences must be created before tables that reference them.

CREATE SEQUENCE IF NOT EXISTS users_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS roles_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS orders_id_seq START 1;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY DEFAULT nextval('users_id_seq'),
    username VARCHAR NOT NULL UNIQUE,
    email VARCHAR NOT NULL UNIQUE,
    display_name VARCHAR,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY DEFAULT nextval('roles_id_seq'),
    name VARCHAR NOT NULL UNIQUE,
    description VARCHAR
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    granted_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY DEFAULT nextval('orders_id_seq'),
    user_id INTEGER NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'pending',
    total_amount DOUBLE NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS products (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    slug VARCHAR NOT NULL UNIQUE,
    price DOUBLE NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key VARCHAR NOT NULL,
    value VARCHAR,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);
