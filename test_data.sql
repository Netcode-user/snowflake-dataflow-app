CREATE DATABASE IF NOT EXISTS dataflow_test_db;
CREATE SCHEMA IF NOT EXISTS dataflow_test_db.sample_data;

USE SCHEMA dataflow_test_db.sample_data;

CREATE OR REPLACE TABLE customers (
    customer_id INTEGER,
    email VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    phone VARCHAR(20),
    signup_date DATE,
    country VARCHAR(50),
    status VARCHAR(20),
    lifetime_value DECIMAL(10,2)
);

INSERT INTO customers VALUES
    (1, 'john.doe@email.com', 'John', 'Doe', '555-0101', '2024-01-15', 'USA', 'Active', 1500.00),
    (2, 'JANE.SMITH@EMAIL.COM', 'Jane', 'Smith', '555-0102', '2024-01-20', 'USA', 'Active', 2300.00),
    (3, 'bob.jones@email.com', 'Bob', 'Jones', NULL, '2024-02-01', 'Canada', 'Active', 950.00),
    (4, 'alice.wilson@email.com', 'Alice', 'Wilson', '555-0104', NULL, 'UK', 'Inactive', NULL),
    (5, 'charlie.brown@email.com', 'Charlie', 'Brown', '555-0105', '2024-02-15', NULL, 'Active', 1750.00),
    (6, 'john.doe@email.com', 'John', 'Doe', '555-0101', '2024-01-15', 'USA', 'Active', 1500.00),
    (7, 'JANE.SMITH@EMAIL.COM', 'Jane', 'Smith', '555-0102', '2024-01-20', 'USA', 'Active', 2300.00),
    (8, NULL, 'David', 'Lee', '555-0108', '2024-03-01', 'USA', 'Active', 890.00),
    (9, 'emma.davis@email.com', NULL, NULL, NULL, '2024-03-10', 'Australia', 'Active', 1200.00),
    (10, 'frank.miller@email.com', 'Frank', 'Miller', '555-0110', '2024-03-15', 'USA', NULL, 0.00),
    (11, 'grace.taylor@email.com', 'Grace', 'Taylor', '555-0111', '2024-03-20', 'Canada', 'Active', 1650.00),
    (12, 'henry.anderson@email.com', 'Henry', 'Anderson', '555-0112', '2024-04-01', 'UK', 'Active', 2100.00),
    (13, 'isabel.thomas@email.com', 'Isabel', 'Thomas', '555-0113', '2024-04-05', 'USA', 'Inactive', 750.00),
    (14, 'jack.jackson@email.com', 'Jack', 'Jackson', NULL, '2024-04-10', 'Canada', 'Active', 1890.00),
    (15, 'kate.white@email.com', 'Kate', 'White', '555-0115', '2024-04-15', 'Australia', 'Active', 2450.00);

CREATE OR REPLACE TABLE products (
    product_id INTEGER,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    supplier_id INTEGER,
    weight_kg DECIMAL(8,2),
    is_active BOOLEAN,
    last_updated TIMESTAMP_NTZ
);

INSERT INTO products VALUES
    (101, 'Laptop Pro 15"', 'Electronics', 1299.99, 45, 1, 2.1, TRUE, '2024-12-01 10:00:00'),
    (102, 'Wireless Mouse', 'electronics', 29.99, 150, 1, 0.15, TRUE, '2024-12-01 10:05:00'),
    (103, 'USB-C Cable', 'ACCESSORIES', 15.99, 200, 2, 0.05, TRUE, '2024-12-01 10:10:00'),
    (104, 'Office Chair', 'Furniture', 349.99, 25, 3, 15.5, TRUE, '2024-12-01 10:15:00'),
    (105, 'Desk Lamp', 'furniture', 59.99, NULL, 3, 1.2, TRUE, '2024-12-01 10:20:00'),
    (106, 'Notebook Set', 'Stationery', 12.99, 500, 4, 0.8, TRUE, '2024-12-01 10:25:00'),
    (107, 'Monitor 27"', 'Electronics', 399.99, 30, 1, 6.5, TRUE, '2024-12-01 10:30:00'),
    (108, 'Keyboard Mechanical', 'Electronics', 129.99, 75, 1, 1.1, TRUE, '2024-12-01 10:35:00'),
    (109, 'Desk Organizer', 'ACCESSORIES', 24.99, 100, NULL, 0.5, FALSE, '2024-12-01 10:40:00'),
    (110, 'Headphones', 'Electronics', 89.99, 60, 1, 0.3, TRUE, '2024-12-01 10:45:00');

CREATE OR REPLACE TABLE orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    payment_method VARCHAR(20),
    shipping_address VARCHAR(200),
    order_status VARCHAR(20)
);

INSERT INTO orders VALUES
    (1001, 1, '2024-11-01', 101, 1, 1299.99, 1299.99, 'Credit Card', '123 Main St, City, USA', 'Delivered'),
    (1002, 2, '2024-11-02', 102, 2, 29.99, 59.98, 'PayPal', '456 Oak Ave, City, USA', 'Delivered'),
    (1003, 3, '2024-11-03', 104, 1, 349.99, 349.99, 'Credit Card', '789 Pine Rd, City, Canada', 'Delivered'),
    (1004, 1, '2024-11-05', 107, 1, 399.99, 399.99, 'Credit Card', '123 Main St, City, USA', 'Shipped'),
    (1005, 5, '2024-11-06', 106, 5, 12.99, 64.95, 'Debit Card', NULL, 'Processing'),
    (1006, 2, '2024-11-08', 110, 1, 89.99, 89.99, 'PayPal', '456 Oak Ave, City, USA', 'Delivered'),
    (1007, 11, '2024-11-10', 108, 1, 129.99, 129.99, 'Credit Card', '321 Elm St, City, Canada', 'Delivered'),
    (1008, 12, '2024-11-12', 103, 3, 15.99, 47.97, 'Credit Card', '654 Maple Dr, City, UK', 'Shipped'),
    (1009, 15, '2024-11-15', 101, 1, 1299.99, 1299.99, 'Bank Transfer', '987 Cedar Ln, City, Australia', 'Processing'),
    (1010, 1, '2024-11-18', 105, 2, 59.99, 119.98, 'Credit Card', '123 Main St, City, USA', 'Delivered');

CREATE OR REPLACE TABLE sales (
    sale_id INTEGER,
    sale_date DATE,
    region VARCHAR(50),
    product_category VARCHAR(50),
    units_sold INTEGER,
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2),
    profit DECIMAL(12,2),
    sales_rep VARCHAR(50)
);

INSERT INTO sales 
SELECT 
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as sale_id,
    DATEADD('day', UNIFORM(0, 90, RANDOM()), '2024-09-01')::DATE as sale_date,
    CASE UNIFORM(1, 4, RANDOM())
        WHEN 1 THEN 'North'
        WHEN 2 THEN 'South'
        WHEN 3 THEN 'East'
        ELSE 'West'
    END as region,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Furniture'
        WHEN 3 THEN 'Stationery'
        WHEN 4 THEN 'Accessories'
        ELSE 'Office Supplies'
    END as product_category,
    UNIFORM(1, 100, RANDOM()) as units_sold,
    UNIFORM(100, 5000, RANDOM()) as revenue,
    UNIFORM(50, 3000, RANDOM()) as cost,
    UNIFORM(50, 2000, RANDOM()) as profit,
    CASE UNIFORM(1, 5, RANDOM())
        WHEN 1 THEN 'John Smith'
        WHEN 2 THEN 'Mary Johnson'
        WHEN 3 THEN 'Bob Williams'
        WHEN 4 THEN 'Alice Brown'
        ELSE 'Charlie Davis'
    END as sales_rep
FROM TABLE(GENERATOR(ROWCOUNT => 100));

CREATE OR REPLACE VIEW customer_summary AS
SELECT 
    country,
    status,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_lifetime_value,
    SUM(lifetime_value) as total_lifetime_value
FROM customers
WHERE country IS NOT NULL
GROUP BY country, status;

CREATE OR REPLACE VIEW product_inventory AS
SELECT 
    category,
    COUNT(*) as product_count,
    SUM(stock_quantity) as total_stock,
    AVG(price) as avg_price,
    SUM(price * stock_quantity) as inventory_value
FROM products
WHERE is_active = TRUE
GROUP BY category;

GRANT USAGE ON DATABASE dataflow_test_db TO PUBLIC;
GRANT USAGE ON SCHEMA dataflow_test_db.sample_data TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA dataflow_test_db.sample_data TO PUBLIC;
GRANT SELECT ON ALL VIEWS IN SCHEMA dataflow_test_db.sample_data TO PUBLIC;

SELECT 'Test data created successfully!' as status;
SELECT 'Customers table: ' || COUNT(*) || ' rows' as info FROM customers;
SELECT 'Products table: ' || COUNT(*) || ' rows' as info FROM products;
SELECT 'Orders table: ' || COUNT(*) || ' rows' as info FROM orders;
SELECT 'Sales table: ' || COUNT(*) || ' rows' as info FROM sales;
