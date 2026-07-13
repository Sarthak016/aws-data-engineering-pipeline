-- Create schema
CREATE SCHEMA ecommerce;

-- Create orders table
CREATE TABLE ecommerce.orders (
    order_id INT,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    amount INT,
    order_date DATE,
    order_category VARCHAR(50)
);
