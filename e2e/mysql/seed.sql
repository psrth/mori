-- seed.sql for MySQL E2E tests
-- Plain SQL statements (no DELIMITER or stored procedures).
-- Each statement is separated by a semicolon.

-- Users table (100 rows)
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255),
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Roles table (10 rows)
CREATE TABLE IF NOT EXISTS roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT
);

-- User Roles table (composite PK)
CREATE TABLE IF NOT EXISTS user_roles (
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, role_id)
);

-- Orders table (200 rows)
CREATE TABLE IF NOT EXISTS orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Products table (CHAR(36) UUID PK, 50 rows)
CREATE TABLE IF NOT EXISTS products (
    id CHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    slug VARCHAR(255) NOT NULL UNIQUE,
    price DECIMAL(10,2) NOT NULL
);

-- Settings table (NO PK, 20 rows)
CREATE TABLE IF NOT EXISTS settings (
    `key` VARCHAR(255) NOT NULL,
    value TEXT,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Seed roles (10 rows)
INSERT INTO roles (name, description) VALUES
    ('superadmin', 'Full system access'),
    ('admin', 'Administrative access'),
    ('editor', 'Content editing'),
    ('viewer', 'Read-only access'),
    ('moderator', 'Content moderation'),
    ('billing', 'Billing access'),
    ('support', 'Support access'),
    ('developer', 'Developer access'),
    ('analyst', 'Analytics access'),
    ('guest', 'Guest access');

-- Seed settings (20 rows)
INSERT INTO settings (`key`, value) VALUES
    ('site.name', 'Mori Test App'),
    ('site.url', 'https://test.example.com'),
    ('site.description', 'E2E test application'),
    ('mail.smtp_host', 'smtp.example.com'),
    ('mail.smtp_port', '587'),
    ('mail.from', 'noreply@example.com'),
    ('auth.session_ttl', '3600'),
    ('auth.max_attempts', '5'),
    ('auth.lockout_duration', '300'),
    ('ui.theme', 'dark'),
    ('ui.language', 'en'),
    ('ui.items_per_page', '25'),
    ('cache.ttl', '600'),
    ('cache.driver', 'redis'),
    ('log.level', 'info'),
    ('log.format', 'json'),
    ('feature.dark_mode', 'true'),
    ('feature.beta_users', 'false'),
    ('rate_limit.requests', '100'),
    ('rate_limit.window', '60');
