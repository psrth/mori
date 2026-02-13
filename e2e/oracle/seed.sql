-- seed.sql for Oracle E2E tests

-- Users table
CREATE TABLE users (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username VARCHAR2(255) NOT NULL UNIQUE,
    email VARCHAR2(255) NOT NULL UNIQUE,
    display_name VARCHAR2(255),
    is_active NUMBER(1) DEFAULT 1 NOT NULL,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
)
/

-- Roles table
CREATE TABLE roles (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR2(255) NOT NULL UNIQUE,
    description VARCHAR2(4000)
)
/

-- User Roles table (composite PK)
CREATE TABLE user_roles (
    user_id NUMBER NOT NULL,
    role_id NUMBER NOT NULL,
    granted_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
    PRIMARY KEY (user_id, role_id)
)
/

-- Orders table
CREATE TABLE orders (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id NUMBER NOT NULL,
    status VARCHAR2(50) DEFAULT 'pending' NOT NULL,
    total_amount NUMBER(12,2) DEFAULT 0 NOT NULL,
    created_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
)
/

-- Products table (UUID PK using RAW(16))
CREATE TABLE products (
    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    name VARCHAR2(255) NOT NULL,
    slug VARCHAR2(255) NOT NULL UNIQUE,
    price NUMBER(10,2) NOT NULL
)
/

-- Settings table (NO PK)
CREATE TABLE settings (
    "KEY" VARCHAR2(255) NOT NULL,
    value VARCHAR2(4000),
    updated_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
)
/

-- Seed roles
INSERT INTO roles (name, description) VALUES ('superadmin', 'Full system access')
/
INSERT INTO roles (name, description) VALUES ('admin', 'Administrative access')
/
INSERT INTO roles (name, description) VALUES ('editor', 'Content editing')
/
INSERT INTO roles (name, description) VALUES ('viewer', 'Read-only access')
/
INSERT INTO roles (name, description) VALUES ('moderator', 'Content moderation')
/
INSERT INTO roles (name, description) VALUES ('billing', 'Billing access')
/
INSERT INTO roles (name, description) VALUES ('support', 'Support access')
/
INSERT INTO roles (name, description) VALUES ('developer', 'Developer access')
/
INSERT INTO roles (name, description) VALUES ('analyst', 'Analytics access')
/
INSERT INTO roles (name, description) VALUES ('guest', 'Guest access')
/

-- Seed users (100 rows)
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO users (username, email, display_name, is_active)
        VALUES (
            'user_' || TO_CHAR(i),
            'user_' || TO_CHAR(i) || '@example.com',
            'User Number ' || TO_CHAR(i),
            CASE WHEN MOD(i, 10) = 0 THEN 0 ELSE 1 END
        );
    END LOOP;
    COMMIT;
END;
/

-- Seed user_roles (~150 rows)
BEGIN
    FOR i IN 1..150 LOOP
        BEGIN
            INSERT INTO user_roles (user_id, role_id)
            VALUES (MOD(i - 1, 100) + 1, MOD(i - 1, 10) + 1);
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX THEN NULL;
        END;
    END LOOP;
    COMMIT;
END;
/

-- Seed orders (200 rows)
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO orders (user_id, status, total_amount)
        VALUES (
            MOD(i - 1, 100) + 1,
            CASE MOD(i, 5)
                WHEN 0 THEN 'completed'
                WHEN 1 THEN 'shipped'
                WHEN 2 THEN 'processing'
                WHEN 3 THEN 'cancelled'
                ELSE 'pending'
            END,
            20.00 + (i * 3.75)
        );
    END LOOP;
    COMMIT;
END;
/

-- Seed products (50 rows)
BEGIN
    FOR i IN 1..50 LOOP
        INSERT INTO products (name, slug, price)
        VALUES (
            'Product ' || TO_CHAR(i),
            'product-' || TO_CHAR(i),
            10.00 + (i * 1.50)
        );
    END LOOP;
    COMMIT;
END;
/

-- Seed settings (20 rows)
INSERT INTO settings ("KEY", value) VALUES ('site.name', 'Mori Test App')
/
INSERT INTO settings ("KEY", value) VALUES ('site.url', 'https://test.example.com')
/
INSERT INTO settings ("KEY", value) VALUES ('site.description', 'E2E test application')
/
INSERT INTO settings ("KEY", value) VALUES ('mail.smtp_host', 'smtp.example.com')
/
INSERT INTO settings ("KEY", value) VALUES ('mail.smtp_port', '587')
/
INSERT INTO settings ("KEY", value) VALUES ('mail.from', 'noreply@example.com')
/
INSERT INTO settings ("KEY", value) VALUES ('auth.session_ttl', '3600')
/
INSERT INTO settings ("KEY", value) VALUES ('auth.max_attempts', '5')
/
INSERT INTO settings ("KEY", value) VALUES ('auth.lockout_duration', '300')
/
INSERT INTO settings ("KEY", value) VALUES ('ui.theme', 'dark')
/
INSERT INTO settings ("KEY", value) VALUES ('ui.language', 'en')
/
INSERT INTO settings ("KEY", value) VALUES ('ui.items_per_page', '25')
/
INSERT INTO settings ("KEY", value) VALUES ('cache.ttl', '600')
/
INSERT INTO settings ("KEY", value) VALUES ('cache.driver', 'redis')
/
INSERT INTO settings ("KEY", value) VALUES ('log.level', 'info')
/
INSERT INTO settings ("KEY", value) VALUES ('log.format', 'json')
/
INSERT INTO settings ("KEY", value) VALUES ('feature.dark_mode', 'true')
/
INSERT INTO settings ("KEY", value) VALUES ('feature.beta_users', 'false')
/
INSERT INTO settings ("KEY", value) VALUES ('rate_limit.requests', '100')
/
INSERT INTO settings ("KEY", value) VALUES ('rate_limit.window', '60')
/
COMMIT
/
