-- All orders
SELECT * FROM ecommerce.orders;

-- Revenue by order category
SELECT 
    order_category,
    COUNT(*) as total_orders,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM ecommerce.orders
GROUP BY order_category
ORDER BY total_revenue DESC;

-- Top 5 customers by spend
SELECT 
    customer_name,
    product,
    amount,
    order_category
FROM ecommerce.orders
ORDER BY amount DESC
LIMIT 5;

-- Athena: query with partition pruning
SELECT * FROM ecommerce_db.orders
WHERE year='2024' AND month='01' AND day='15';

-- Athena: combined partition and row level filter
SELECT product, amount
FROM ecommerce_db.orders
WHERE year='2024' AND month='01' AND day='16'
AND amount > 1000;
