CREATE TABLE users (
    id          SERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    full_name   TEXT,
    join_date   DATE NOT NULL
);

CREATE TABLE sellers (
    id          SERIAL PRIMARY KEY,
    user_id     INT UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    rating      NUMERIC(3,2) DEFAULT 0
);

CREATE TABLE categories (
    id   SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE products (
    id           SERIAL PRIMARY KEY,
    category_id  INT REFERENCES categories(id),
    seller_id    INT REFERENCES sellers(id),
    name         TEXT NOT NULL,
    description  TEXT,
    price_cents  INT NOT NULL
);

CREATE TABLE orders (
    id        SERIAL PRIMARY KEY,
    user_id   INT REFERENCES users(id),
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE order_items (
    order_id   INT REFERENCES orders(id) ON DELETE CASCADE,
    product_id INT REFERENCES products(id),
    quantity   INT NOT NULL,
    price_cents INT NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE product_embeddings (
    product_id INT PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    embedding  vector(384)
);

CREATE INDEX idx_products_category      ON products(category_id);
CREATE INDEX idx_products_fulltext      ON products USING gin(to_tsvector('simple', name || ' ' || coalesce(description,'')));