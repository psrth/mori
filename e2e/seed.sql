-- ============================================================
-- Mori E2E Test Seed Database
-- 18 tables, ~70k+ rows, diverse PK types, complex relationships
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- 1. users (serial PK, many column types)
-- ============================================================
CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    username      TEXT NOT NULL UNIQUE,
    email         TEXT NOT NULL UNIQUE,
    display_name  TEXT,
    password_hash TEXT NOT NULL,
    is_active     BOOLEAN NOT NULL DEFAULT true,
    login_count   INTEGER NOT NULL DEFAULT 0,
    metadata      JSONB DEFAULT '{}',
    tags          TEXT[] DEFAULT '{}',
    last_login_ip INET,
    created_at    TIMESTAMP NOT NULL DEFAULT now(),
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 2. user_profiles (serial PK, 1:1 with users)
-- ============================================================
CREATE TABLE user_profiles (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL UNIQUE REFERENCES users(id),
    bio        TEXT,
    avatar_url TEXT,
    location   TEXT,
    website    TEXT,
    birth_date DATE,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 3. organizations (serial PK)
-- ============================================================
CREATE TABLE organizations (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    slug        TEXT NOT NULL UNIQUE,
    description TEXT,
    website     TEXT,
    is_verified BOOLEAN NOT NULL DEFAULT false,
    settings    JSONB DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 4. org_members (composite PK: org_id + user_id)
-- ============================================================
CREATE TABLE org_members (
    org_id    INTEGER NOT NULL REFERENCES organizations(id),
    user_id   INTEGER NOT NULL REFERENCES users(id),
    role      TEXT NOT NULL DEFAULT 'member',
    joined_at TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, user_id)
);

-- ============================================================
-- 5. roles (serial PK)
-- ============================================================
CREATE TABLE roles (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    permissions TEXT[] DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 6. user_roles (composite PK: user_id + role_id)
-- ============================================================
CREATE TABLE user_roles (
    user_id    INTEGER NOT NULL REFERENCES users(id),
    role_id    INTEGER NOT NULL REFERENCES roles(id),
    granted_at TIMESTAMP NOT NULL DEFAULT now(),
    granted_by INTEGER REFERENCES users(id),
    PRIMARY KEY (user_id, role_id)
);

-- ============================================================
-- 7. products (UUID PK)
-- ============================================================
CREATE TABLE products (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    description TEXT,
    price       NUMERIC(10,2) NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    stock       INTEGER NOT NULL DEFAULT 0,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    attributes  JSONB DEFAULT '{}',
    tags        TEXT[] DEFAULT '{}',
    created_at  TIMESTAMP NOT NULL DEFAULT now(),
    updated_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 8. categories (serial PK, self-referencing FK)
-- ============================================================
CREATE TABLE categories (
    id        SERIAL PRIMARY KEY,
    name      TEXT NOT NULL,
    slug      TEXT NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES categories(id),
    depth     INTEGER NOT NULL DEFAULT 0,
    path      TEXT NOT NULL DEFAULT ''
);

-- ============================================================
-- 9. orders (bigserial PK)
-- ============================================================
CREATE TABLE orders (
    id              BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id),
    status          TEXT NOT NULL DEFAULT 'pending',
    total_amount    NUMERIC(12,2) NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'USD',
    shipping_addr   JSONB,
    notes           TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 10. order_items (serial PK, FK to orders + products)
-- ============================================================
CREATE TABLE order_items (
    id          SERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES orders(id),
    product_id  UUID NOT NULL REFERENCES products(id),
    quantity    INTEGER NOT NULL DEFAULT 1,
    unit_price  NUMERIC(10,2) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 11. payments (UUID PK, FK to orders)
-- ============================================================
CREATE TABLE payments (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id       BIGINT NOT NULL REFERENCES orders(id),
    amount         NUMERIC(12,2) NOT NULL,
    currency       TEXT NOT NULL DEFAULT 'USD',
    method         TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    transaction_id TEXT UNIQUE,
    metadata       JSONB DEFAULT '{}',
    created_at     TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 12. posts (serial PK, FK to users)
-- ============================================================
CREATE TABLE posts (
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER NOT NULL REFERENCES users(id),
    title        TEXT NOT NULL,
    body         TEXT NOT NULL,
    slug         TEXT NOT NULL UNIQUE,
    status       TEXT NOT NULL DEFAULT 'draft',
    view_count   BIGINT NOT NULL DEFAULT 0,
    metadata     JSONB DEFAULT '{}',
    published_at TIMESTAMP,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 13. comments (serial PK, self-referencing FK for threaded comments)
-- ============================================================
CREATE TABLE comments (
    id         SERIAL PRIMARY KEY,
    post_id    INTEGER NOT NULL REFERENCES posts(id),
    user_id    INTEGER NOT NULL REFERENCES users(id),
    parent_id  INTEGER REFERENCES comments(id),
    body       TEXT NOT NULL,
    is_edited  BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 14. tags (serial PK)
-- ============================================================
CREATE TABLE tags (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE
);

-- ============================================================
-- 15. post_tags (composite PK: post_id + tag_id)
-- ============================================================
CREATE TABLE post_tags (
    post_id INTEGER NOT NULL REFERENCES posts(id),
    tag_id  INTEGER NOT NULL REFERENCES tags(id),
    PRIMARY KEY (post_id, tag_id)
);

-- ============================================================
-- 16. audit_log (bigserial PK, high volume)
-- ============================================================
CREATE TABLE audit_log (
    id         BIGSERIAL PRIMARY KEY,
    user_id    INTEGER,
    action     TEXT NOT NULL,
    resource   TEXT NOT NULL,
    resource_id TEXT,
    details    JSONB DEFAULT '{}',
    ip_address INET,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 17. settings (NO PRIMARY KEY - edge case)
-- ============================================================
CREATE TABLE settings (
    key        TEXT NOT NULL,
    value      TEXT,
    category   TEXT NOT NULL DEFAULT 'general',
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- 18. sessions (UUID PK)
-- ============================================================
CREATE TABLE sessions (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    INTEGER NOT NULL REFERENCES users(id),
    token      TEXT NOT NULL UNIQUE,
    user_agent TEXT,
    ip_address INET,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_user_profiles_user_id ON user_profiles(user_id);
CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_status ON posts(status);
CREATE INDEX idx_comments_post_id ON comments(post_id);
CREATE INDEX idx_comments_user_id ON comments(user_id);
CREATE INDEX idx_comments_parent_id ON comments(parent_id);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_audit_log_user_id ON audit_log(user_id);
CREATE INDEX idx_audit_log_action ON audit_log(action);
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);
CREATE INDEX idx_sessions_user_id ON sessions(user_id);
CREATE INDEX idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX idx_categories_parent_id ON categories(parent_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);
CREATE INDEX idx_payments_status ON payments(status);

-- ============================================================
-- SEED DATA
-- ============================================================

-- users: 500 rows
INSERT INTO users (username, email, display_name, password_hash, is_active, login_count, metadata, tags, last_login_ip, created_at, updated_at)
SELECT
    'user_' || i,
    'user_' || i || '@example.com',
    'User Number ' || i,
    'hash_' || md5(i::text),
    (i % 10 != 0),
    (i * 7) % 100,
    jsonb_build_object(
        'theme', CASE WHEN i % 2 = 0 THEN 'dark' ELSE 'light' END,
        'lang', CASE WHEN i % 3 = 0 THEN 'es' WHEN i % 3 = 1 THEN 'en' ELSE 'fr' END,
        'notifications', (i % 2 = 0)
    ),
    CASE
        WHEN i % 3 = 0 THEN ARRAY['admin','staff']
        WHEN i % 3 = 1 THEN ARRAY['user']
        ELSE ARRAY[]::TEXT[]
    END,
    ('192.168.1.' || (i % 255 + 1))::inet,
    now() - (i || ' hours')::interval,
    now() - (i || ' hours')::interval
FROM generate_series(1, 500) AS i;

-- user_profiles: 500 rows (1:1 with users)
INSERT INTO user_profiles (user_id, bio, avatar_url, location, website, birth_date, created_at)
SELECT
    i,
    CASE WHEN i % 5 = 0 THEN NULL ELSE 'Bio for user_' || i || '. Enjoys coding and testing.' END,
    'https://avatars.example.com/' || i || '.jpg',
    CASE
        WHEN i % 4 = 0 THEN 'New York'
        WHEN i % 4 = 1 THEN 'London'
        WHEN i % 4 = 2 THEN 'Tokyo'
        ELSE 'Berlin'
    END,
    CASE WHEN i % 3 = 0 THEN 'https://user' || i || '.example.com' ELSE NULL END,
    '1980-01-01'::date + (i || ' days')::interval,
    now() - (i || ' hours')::interval
FROM generate_series(1, 500) AS i;

-- organizations: 50 rows
INSERT INTO organizations (name, slug, description, website, is_verified, settings, created_at)
SELECT
    'Organization ' || i,
    'org-' || i,
    'Description for organization ' || i || '. A great team.',
    'https://org' || i || '.example.com',
    (i % 3 = 0),
    jsonb_build_object(
        'plan', CASE WHEN i % 3 = 0 THEN 'enterprise' WHEN i % 3 = 1 THEN 'pro' ELSE 'free' END,
        'seats', (i * 10)
    ),
    now() - (i || ' days')::interval
FROM generate_series(1, 50) AS i;

-- org_members: ~1000 rows (composite PK)
INSERT INTO org_members (org_id, user_id, role, joined_at)
SELECT
    ((i - 1) % 50) + 1,
    ((i - 1) % 500) + 1,
    CASE
        WHEN i % 20 = 0 THEN 'owner'
        WHEN i % 20 < 3 THEN 'admin'
        ELSE 'member'
    END,
    now() - (i || ' hours')::interval
FROM generate_series(1, 1000) AS i
ON CONFLICT DO NOTHING;

-- roles: 10 rows
INSERT INTO roles (name, description, permissions) VALUES
    ('superadmin', 'Full system access', ARRAY['*']),
    ('admin', 'Administrative access', ARRAY['read','write','delete','manage_users']),
    ('editor', 'Content editing', ARRAY['read','write']),
    ('viewer', 'Read-only access', ARRAY['read']),
    ('moderator', 'Content moderation', ARRAY['read','moderate','delete']),
    ('billing', 'Billing access', ARRAY['read','billing']),
    ('support', 'Support access', ARRAY['read','support']),
    ('developer', 'Developer access', ARRAY['read','write','deploy']),
    ('analyst', 'Analytics access', ARRAY['read','analytics']),
    ('guest', 'Guest access', ARRAY['read']);

-- user_roles: ~1500 rows (composite PK)
INSERT INTO user_roles (user_id, role_id, granted_at, granted_by)
SELECT
    ((i - 1) % 500) + 1,
    ((i - 1) % 10) + 1,
    now() - (i || ' minutes')::interval,
    CASE WHEN i % 5 = 0 THEN NULL ELSE ((i * 3) % 500) + 1 END
FROM generate_series(1, 1500) AS i
ON CONFLICT DO NOTHING;

-- categories: 30 rows (self-referencing, 3-level hierarchy)
INSERT INTO categories (id, name, slug, parent_id, depth, path) VALUES
    (1,  'Electronics',    'electronics',     NULL, 0, 'electronics'),
    (2,  'Clothing',       'clothing',        NULL, 0, 'clothing'),
    (3,  'Books',          'books',           NULL, 0, 'books'),
    (4,  'Home & Garden',  'home-garden',     NULL, 0, 'home-garden'),
    (5,  'Sports',         'sports',          NULL, 0, 'sports'),
    (6,  'Phones',         'phones',          1,    1, 'electronics/phones'),
    (7,  'Laptops',        'laptops',         1,    1, 'electronics/laptops'),
    (8,  'Tablets',         'tablets',         1,    1, 'electronics/tablets'),
    (9,  'Men',            'men',             2,    1, 'clothing/men'),
    (10, 'Women',          'women',           2,    1, 'clothing/women'),
    (11, 'Fiction',        'fiction',          3,    1, 'books/fiction'),
    (12, 'Non-Fiction',    'non-fiction',      3,    1, 'books/non-fiction'),
    (13, 'Furniture',      'furniture',        4,    1, 'home-garden/furniture'),
    (14, 'Gardening',      'gardening',        4,    1, 'home-garden/gardening'),
    (15, 'Team Sports',    'team-sports',      5,    1, 'sports/team-sports'),
    (16, 'iPhones',        'iphones',          6,    2, 'electronics/phones/iphones'),
    (17, 'Android',        'android',          6,    2, 'electronics/phones/android'),
    (18, 'Gaming Laptops', 'gaming-laptops',   7,    2, 'electronics/laptops/gaming-laptops'),
    (19, 'Shirts',         'shirts',           9,    2, 'clothing/men/shirts'),
    (20, 'Pants',          'pants',            9,    2, 'clothing/men/pants'),
    (21, 'Dresses',        'dresses',         10,    2, 'clothing/women/dresses'),
    (22, 'Sci-Fi',         'sci-fi',          11,    2, 'books/fiction/sci-fi'),
    (23, 'Mystery',        'mystery',         11,    2, 'books/fiction/mystery'),
    (24, 'Biography',      'biography',       12,    2, 'books/non-fiction/biography'),
    (25, 'Sofas',          'sofas',           13,    2, 'home-garden/furniture/sofas'),
    (26, 'Tables',         'tables',          13,    2, 'home-garden/furniture/tables'),
    (27, 'Football',       'football',        15,    2, 'sports/team-sports/football'),
    (28, 'Basketball',     'basketball',      15,    2, 'sports/team-sports/basketball'),
    (29, 'Soccer',         'soccer',          15,    2, 'sports/team-sports/soccer'),
    (30, 'Tennis',         'tennis',           5,    1, 'sports/tennis');
SELECT setval('categories_id_seq', 30);

-- products: 200 rows (UUID PK, deterministic via uuid_generate_v5)
INSERT INTO products (id, name, slug, description, price, currency, stock, is_active, attributes, tags, created_at, updated_at)
SELECT
    uuid_generate_v5(uuid_ns_url(), 'product-' || i),
    'Product ' || i,
    'product-' || i,
    'Description for product ' || i || '. High quality item.',
    (10.00 + (i * 1.50))::numeric(10,2),
    CASE WHEN i % 3 = 0 THEN 'EUR' WHEN i % 3 = 1 THEN 'USD' ELSE 'GBP' END,
    (i * 5) % 200,
    (i % 7 != 0),
    jsonb_build_object(
        'weight', (i * 0.5)::text || 'kg',
        'color', CASE
            WHEN i % 5 = 0 THEN 'red'
            WHEN i % 5 = 1 THEN 'blue'
            WHEN i % 5 = 2 THEN 'green'
            WHEN i % 5 = 3 THEN 'black'
            ELSE 'white'
        END,
        'rating', (i % 5) + 1
    ),
    CASE WHEN i % 2 = 0 THEN ARRAY['sale','featured'] ELSE ARRAY['new'] END,
    now() - (i || ' hours')::interval,
    now() - (i || ' hours')::interval
FROM generate_series(1, 200) AS i;

-- orders: 2000 rows (bigserial PK)
INSERT INTO orders (user_id, status, total_amount, currency, shipping_addr, notes, created_at, updated_at)
SELECT
    ((i - 1) % 500) + 1,
    CASE
        WHEN i % 5 = 0 THEN 'completed'
        WHEN i % 5 = 1 THEN 'shipped'
        WHEN i % 5 = 2 THEN 'processing'
        WHEN i % 5 = 3 THEN 'cancelled'
        ELSE 'pending'
    END,
    (20.00 + (i * 3.75))::numeric(12,2),
    'USD',
    jsonb_build_object(
        'street', i || ' Main St',
        'city', CASE WHEN i % 3 = 0 THEN 'New York' WHEN i % 3 = 1 THEN 'Los Angeles' ELSE 'Chicago' END,
        'zip', '1' || lpad((i % 10000)::text, 4, '0')
    ),
    CASE WHEN i % 10 = 0 THEN 'Rush delivery requested' ELSE NULL END,
    now() - (i || ' hours')::interval,
    now() - (i || ' hours')::interval
FROM generate_series(1, 2000) AS i;

-- order_items: 5000 rows
INSERT INTO order_items (order_id, product_id, quantity, unit_price, created_at)
SELECT
    ((i - 1) % 2000) + 1,
    uuid_generate_v5(uuid_ns_url(), 'product-' || (((i - 1) % 200) + 1)),
    ((i - 1) % 5) + 1,
    (10.00 + (i * 0.99))::numeric(10,2),
    now() - (i || ' minutes')::interval
FROM generate_series(1, 5000) AS i;

-- payments: 1500 rows (UUID PK)
INSERT INTO payments (order_id, amount, currency, method, status, transaction_id, metadata, created_at)
SELECT
    ((i - 1) % 2000) + 1,
    (20.00 + (i * 2.25))::numeric(12,2),
    'USD',
    CASE
        WHEN i % 4 = 0 THEN 'credit_card'
        WHEN i % 4 = 1 THEN 'paypal'
        WHEN i % 4 = 2 THEN 'bank_transfer'
        ELSE 'crypto'
    END,
    CASE
        WHEN i % 3 = 0 THEN 'completed'
        WHEN i % 3 = 1 THEN 'pending'
        ELSE 'failed'
    END,
    'txn_' || md5(i::text),
    jsonb_build_object('gateway', 'stripe', 'ref', 'ref_' || i),
    now() - (i || ' minutes')::interval
FROM generate_series(1, 1500) AS i;

-- posts: 1000 rows
INSERT INTO posts (user_id, title, body, slug, status, view_count, metadata, published_at, created_at, updated_at)
SELECT
    ((i - 1) % 500) + 1,
    'Post Title ' || i,
    'This is the body of post ' || i || '. ' || repeat('Lorem ipsum dolor sit amet. ', 5),
    'post-' || i,
    CASE
        WHEN i % 4 = 0 THEN 'published'
        WHEN i % 4 = 1 THEN 'draft'
        WHEN i % 4 = 2 THEN 'archived'
        ELSE 'published'
    END,
    (i * 17) % 10000,
    jsonb_build_object('reading_time', (i % 20) + 1, 'featured', i % 10 = 0),
    CASE WHEN i % 4 IN (0, 3) THEN now() - (i || ' hours')::interval ELSE NULL END,
    now() - (i || ' hours')::interval,
    now() - (i || ' hours')::interval
FROM generate_series(1, 1000) AS i;

-- comments: 3000 rows (some self-referencing for threading)
INSERT INTO comments (post_id, user_id, parent_id, body, is_edited, created_at, updated_at)
SELECT
    ((i - 1) % 1000) + 1,
    ((i - 1) % 500) + 1,
    CASE
        WHEN i > 500 AND i % 5 = 0 THEN (i % 499) + 1
        ELSE NULL
    END,
    'Comment body ' || i || '. ' ||
    CASE
        WHEN i % 3 = 0 THEN 'Great post!'
        WHEN i % 3 = 1 THEN 'Interesting perspective.'
        ELSE 'I disagree with this take.'
    END,
    (i % 8 = 0),
    now() - (i || ' minutes')::interval,
    now() - (i || ' minutes')::interval
FROM generate_series(1, 3000) AS i;

-- tags: 50 rows
INSERT INTO tags (name, slug)
SELECT 'tag-' || i, 'tag-' || i
FROM generate_series(1, 50) AS i;

-- post_tags: ~3000 rows (composite PK)
INSERT INTO post_tags (post_id, tag_id)
SELECT
    ((i - 1) % 1000) + 1,
    ((i - 1) % 50) + 1
FROM generate_series(1, 3000) AS i
ON CONFLICT DO NOTHING;

-- audit_log: 50000 rows (high volume)
INSERT INTO audit_log (user_id, action, resource, resource_id, details, ip_address, created_at)
SELECT
    CASE WHEN i % 20 = 0 THEN NULL ELSE ((i - 1) % 500) + 1 END,
    CASE
        WHEN i % 6 = 0 THEN 'create'
        WHEN i % 6 = 1 THEN 'read'
        WHEN i % 6 = 2 THEN 'update'
        WHEN i % 6 = 3 THEN 'delete'
        WHEN i % 6 = 4 THEN 'login'
        ELSE 'logout'
    END,
    CASE
        WHEN i % 4 = 0 THEN 'user'
        WHEN i % 4 = 1 THEN 'post'
        WHEN i % 4 = 2 THEN 'order'
        ELSE 'product'
    END,
    ((i % 500) + 1)::text,
    jsonb_build_object(
        'ip', '10.0.' || (i % 256) || '.' || ((i * 3) % 256),
        'browser', CASE
            WHEN i % 3 = 0 THEN 'Chrome'
            WHEN i % 3 = 1 THEN 'Firefox'
            ELSE 'Safari'
        END
    ),
    ('10.0.' || (i % 256) || '.' || ((i * 3) % 256))::inet,
    now() - (i || ' seconds')::interval
FROM generate_series(1, 50000) AS i;

-- settings: 20 rows (NO PRIMARY KEY)
INSERT INTO settings (key, value, category, updated_at) VALUES
    ('site.name', 'Mori Test App', 'general', now()),
    ('site.url', 'https://test.example.com', 'general', now()),
    ('site.description', 'E2E test application', 'general', now()),
    ('mail.smtp_host', 'smtp.example.com', 'mail', now()),
    ('mail.smtp_port', '587', 'mail', now()),
    ('mail.from', 'noreply@example.com', 'mail', now()),
    ('auth.session_ttl', '3600', 'auth', now()),
    ('auth.max_attempts', '5', 'auth', now()),
    ('auth.lockout_duration', '300', 'auth', now()),
    ('ui.theme', 'dark', 'ui', now()),
    ('ui.language', 'en', 'ui', now()),
    ('ui.items_per_page', '25', 'ui', now()),
    ('cache.ttl', '600', 'cache', now()),
    ('cache.driver', 'redis', 'cache', now()),
    ('log.level', 'info', 'logging', now()),
    ('log.format', 'json', 'logging', now()),
    ('feature.dark_mode', 'true', 'features', now()),
    ('feature.beta_users', 'false', 'features', now()),
    ('rate_limit.requests', '100', 'rate_limit', now()),
    ('rate_limit.window', '60', 'rate_limit', now());

-- sessions: 1000 rows (UUID PK)
INSERT INTO sessions (user_id, token, user_agent, ip_address, expires_at, created_at)
SELECT
    ((i - 1) % 500) + 1,
    'token_' || md5(i::text || 'session_salt'),
    CASE
        WHEN i % 3 = 0 THEN 'Mozilla/5.0 (Windows NT 10.0) Chrome/120.0'
        WHEN i % 3 = 1 THEN 'Mozilla/5.0 (Macintosh) Firefox/121.0'
        ELSE 'Mozilla/5.0 (iPhone) Safari/17.2'
    END,
    ('10.0.' || (i % 256) || '.' || ((i * 7) % 256))::inet,
    now() + ((i * 3600) || ' seconds')::interval,
    now() - (i || ' minutes')::interval
FROM generate_series(1, 1000) AS i;

-- ============================================================
-- Verify seed counts
-- ============================================================
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT 'users' AS t, count(*) AS c FROM users
        UNION ALL SELECT 'user_profiles', count(*) FROM user_profiles
        UNION ALL SELECT 'organizations', count(*) FROM organizations
        UNION ALL SELECT 'org_members', count(*) FROM org_members
        UNION ALL SELECT 'roles', count(*) FROM roles
        UNION ALL SELECT 'user_roles', count(*) FROM user_roles
        UNION ALL SELECT 'products', count(*) FROM products
        UNION ALL SELECT 'categories', count(*) FROM categories
        UNION ALL SELECT 'orders', count(*) FROM orders
        UNION ALL SELECT 'order_items', count(*) FROM order_items
        UNION ALL SELECT 'payments', count(*) FROM payments
        UNION ALL SELECT 'posts', count(*) FROM posts
        UNION ALL SELECT 'comments', count(*) FROM comments
        UNION ALL SELECT 'tags', count(*) FROM tags
        UNION ALL SELECT 'post_tags', count(*) FROM post_tags
        UNION ALL SELECT 'audit_log', count(*) FROM audit_log
        UNION ALL SELECT 'settings', count(*) FROM settings
        UNION ALL SELECT 'sessions', count(*) FROM sessions
    LOOP
        RAISE NOTICE 'Table %: % rows', r.t, r.c;
    END LOOP;
END $$;
