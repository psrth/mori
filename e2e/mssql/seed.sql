-- seed.sql for MSSQL E2E tests
-- T-SQL batch separated by GO

-- Users table (100 rows)
CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(255) NOT NULL UNIQUE,
    email NVARCHAR(255) NOT NULL UNIQUE,
    display_name NVARCHAR(255),
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

-- Roles table (10 rows)
CREATE TABLE roles (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL UNIQUE,
    description NVARCHAR(MAX)
);
GO

-- User Roles table (composite PK, ~150 rows)
CREATE TABLE user_roles (
    user_id INT NOT NULL,
    role_id INT NOT NULL,
    granted_at DATETIME2 NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (user_id, role_id)
);
GO

-- Orders table (200 rows)
CREATE TABLE orders (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id INT NOT NULL,
    status NVARCHAR(50) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

-- Products table (UUID PK, 50 rows)
CREATE TABLE products (
    id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    name NVARCHAR(255) NOT NULL,
    slug NVARCHAR(255) NOT NULL UNIQUE,
    price DECIMAL(10,2) NOT NULL
);
GO

-- Settings table (NO PK, 20 rows)
CREATE TABLE settings (
    [key] NVARCHAR(255) NOT NULL,
    value NVARCHAR(MAX),
    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
);
GO

-- Seed users (100 rows)
DECLARE @i INT = 1;
WHILE @i <= 100
BEGIN
    INSERT INTO users (username, email, display_name, is_active)
    VALUES (
        'user_' + CAST(@i AS NVARCHAR(10)),
        'user_' + CAST(@i AS NVARCHAR(10)) + '@example.com',
        'User Number ' + CAST(@i AS NVARCHAR(10)),
        CASE WHEN @i % 10 = 0 THEN 0 ELSE 1 END
    );
    SET @i = @i + 1;
END
GO

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
GO

-- Seed user_roles (~150 rows)
DECLARE @j INT = 1;
WHILE @j <= 150
BEGIN
    BEGIN TRY
        INSERT INTO user_roles (user_id, role_id)
        VALUES (((@j - 1) % 100) + 1, ((@j - 1) % 10) + 1);
    END TRY
    BEGIN CATCH
        -- Ignore duplicate key errors
    END CATCH
    SET @j = @j + 1;
END
GO

-- Seed orders (200 rows)
DECLARE @k INT = 1;
WHILE @k <= 200
BEGIN
    INSERT INTO orders (user_id, status, total_amount)
    VALUES (
        ((@k - 1) % 100) + 1,
        CASE @k % 5
            WHEN 0 THEN 'completed'
            WHEN 1 THEN 'shipped'
            WHEN 2 THEN 'processing'
            WHEN 3 THEN 'cancelled'
            ELSE 'pending'
        END,
        20.00 + (@k * 3.75)
    );
    SET @k = @k + 1;
END
GO

-- Seed products (50 rows)
DECLARE @m INT = 1;
WHILE @m <= 50
BEGIN
    INSERT INTO products (name, slug, price)
    VALUES (
        'Product ' + CAST(@m AS NVARCHAR(10)),
        'product-' + CAST(@m AS NVARCHAR(10)),
        10.00 + (@m * 1.50)
    );
    SET @m = @m + 1;
END
GO

-- Seed settings (20 rows)
INSERT INTO settings ([key], value) VALUES
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
GO
