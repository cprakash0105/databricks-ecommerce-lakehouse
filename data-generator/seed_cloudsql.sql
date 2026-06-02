-- Operational tables in Cloud SQL (PostgreSQL)
-- These are queried via Lakehouse Federation (data virtualization)

CREATE TABLE IF NOT EXISTS active_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id UUID NOT NULL,
    current_page VARCHAR(500),
    cart_value DECIMAL(10,2) DEFAULT 0,
    cart_items INTEGER DEFAULT 0,
    last_activity_ts TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    stock_quantity INTEGER DEFAULT 0,
    warehouse_location VARCHAR(100),
    last_restock_date DATE,
    unit_price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(10),
    segment VARCHAR(20),
    signup_date DATE DEFAULT CURRENT_DATE
);

-- Indexes for federation query pushdown
CREATE INDEX idx_sessions_customer ON active_sessions(customer_id);
CREATE INDEX idx_sessions_activity ON active_sessions(last_activity_ts);
CREATE INDEX idx_inventory_category ON inventory(category);
