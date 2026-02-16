-- seed.sql for MariaDB E2E tests
-- MariaDB/MySQL dialect

-- Users table (100 rows)
CREATE TABLE users (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    username     VARCHAR(255) NOT NULL UNIQUE,
    email        VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(255),
    is_active    TINYINT(1) NOT NULL DEFAULT 1,
    created_at   DATETIME NOT NULL DEFAULT NOW()
);

-- Roles table (10 rows)
CREATE TABLE roles (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(255) NOT NULL UNIQUE,
    description TEXT
);

-- User Roles table (composite PK, ~150 rows)
CREATE TABLE user_roles (
    user_id    INT NOT NULL,
    role_id    INT NOT NULL,
    granted_at DATETIME NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, role_id)
);

-- Orders table (200 rows)
CREATE TABLE orders (
    id           BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id      INT NOT NULL,
    status       VARCHAR(50) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    created_at   DATETIME NOT NULL DEFAULT NOW()
);

-- Products table (UUID PK via CHAR(36), 50 rows)
CREATE TABLE products (
    id    CHAR(36) PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    slug  VARCHAR(255) NOT NULL UNIQUE,
    price DECIMAL(10,2) NOT NULL
);

-- Settings table (NO PK, 20 rows)
CREATE TABLE settings (
    `key`      VARCHAR(255) NOT NULL,
    value      TEXT,
    updated_at DATETIME NOT NULL DEFAULT NOW()
);

-- Seed users (100 rows)
DELIMITER $$
CREATE PROCEDURE seed_users()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 100 DO
        INSERT INTO users (username, email, display_name, is_active)
        VALUES (
            CONCAT('user_', i),
            CONCAT('user_', i, '@example.com'),
            CONCAT('User Number ', i),
            IF(i MOD 10 = 0, 0, 1)
        );
        SET i = i + 1;
    END WHILE;
END$$
DELIMITER ;

CALL seed_users();
DROP PROCEDURE seed_users;

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

-- Seed user_roles (~150 rows, ignore duplicates)
DELIMITER $$
CREATE PROCEDURE seed_user_roles()
BEGIN
    DECLARE j INT DEFAULT 1;
    WHILE j <= 150 DO
        INSERT IGNORE INTO user_roles (user_id, role_id)
        VALUES (((j - 1) MOD 100) + 1, ((j - 1) MOD 10) + 1);
        SET j = j + 1;
    END WHILE;
END$$
DELIMITER ;

CALL seed_user_roles();
DROP PROCEDURE seed_user_roles;

-- Seed orders (200 rows)
DELIMITER $$
CREATE PROCEDURE seed_orders()
BEGIN
    DECLARE k INT DEFAULT 1;
    WHILE k <= 200 DO
        INSERT INTO orders (user_id, status, total_amount)
        VALUES (
            ((k - 1) MOD 100) + 1,
            CASE k MOD 5
                WHEN 0 THEN 'completed'
                WHEN 1 THEN 'shipped'
                WHEN 2 THEN 'processing'
                WHEN 3 THEN 'cancelled'
                ELSE 'pending'
            END,
            20.00 + (k * 3.75)
        );
        SET k = k + 1;
    END WHILE;
END$$
DELIMITER ;

CALL seed_orders();
DROP PROCEDURE seed_orders;

-- Seed products (50 rows with deterministic UUIDs)
DELIMITER $$
CREATE PROCEDURE seed_products()
BEGIN
    DECLARE m INT DEFAULT 1;
    WHILE m <= 50 DO
        INSERT INTO products (id, name, slug, price)
        VALUES (
            UUID(),
            CONCAT('Product ', m),
            CONCAT('product-', m),
            10.00 + (m * 1.50)
        );
        SET m = m + 1;
    END WHILE;
END$$
DELIMITER ;

CALL seed_products();
DROP PROCEDURE seed_products;

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
