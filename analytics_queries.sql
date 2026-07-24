USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA ICEBERG_SCHEMA;


-- ── Query 1: Record Count Validation ─────────────────────────

SELECT 'customers' AS table_name, COUNT(*) AS record_count FROM customers
UNION ALL
SELECT 'products',                COUNT(*) FROM products
UNION ALL
SELECT 'orders',                  COUNT(*) FROM orders
ORDER BY table_name;


-- ── Query 2: Total Revenue by Product Category ───────────────
SELECT
    p.category,
    COUNT(o.order_id)                   AS total_orders,
    SUM(o.quantity)                     AS units_sold,
    ROUND(SUM(o.total_amount), 2)       AS total_revenue,
    ROUND(AVG(o.total_amount), 2)       AS avg_order_value
FROM orders   o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;


-- ── Query 3: Top 10 Customers by Lifetime Spend ──────────────

SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name   AS full_name,
    c.email,
    c.country,
    COUNT(o.order_id)                     AS order_count,
    ROUND(SUM(o.total_amount), 2)         AS lifetime_spend,
    c.loyalty_points
FROM customers c
JOIN orders o ON CAST(c.customer_id AS VARCHAR) = o.customer_id
GROUP BY
    c.customer_id, c.first_name, c.last_name,
    c.email, c.country, c.loyalty_points
ORDER BY lifetime_spend DESC
LIMIT 10;


-- ── Query 4: Monthly Revenue Trend ───────────────────────────

SELECT
    DATE_TRUNC('month', order_date)     AS order_month,
    COUNT(order_id)                     AS order_count,
    SUM(quantity)                       AS units_sold,
    ROUND(SUM(total_amount), 2)         AS monthly_revenue
FROM orders
GROUP BY order_month
ORDER BY order_month;


-- ── Query 5: Order Status Breakdown ──────────────────────────

SELECT
    order_status,
    COUNT(*)                                                      AS order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)           AS pct_of_total,
    ROUND(SUM(total_amount), 2)                                   AS total_value
FROM orders
GROUP BY order_status
ORDER BY order_count DESC;


-- ── Query 6: Product Inventory Health ────────────────────────

SELECT
    product_id,
    product_name,
    category,
    brand,
    ROUND(price, 2)   AS price,
    stock_quantity,
    CASE
        WHEN stock_quantity = 0         THEN 'Out of Stock'
        WHEN stock_quantity < 10        THEN 'Critical — Reorder Now'
        WHEN stock_quantity < 50        THEN 'Low Stock'
        ELSE                                 'OK'
    END AS stock_status
FROM products
ORDER BY stock_quantity ASC;


-- ── Query 7: Revenue by Country ──────────────────────────────

SELECT
    c.country,
    COUNT(DISTINCT c.customer_id)       AS customer_count,
    COUNT(o.order_id)                   AS total_orders,
    ROUND(SUM(o.total_amount), 2)       AS total_revenue,
    ROUND(AVG(o.total_amount), 2)       AS avg_order_value
FROM customers c
JOIN orders o ON CAST(c.customer_id AS VARCHAR) = o.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;


-- ── Query 8: Customer Segmentation by Purchase Behavior ──────

SELECT
    buyer_segment,
    COUNT(*)                            AS customer_count,
    ROUND(AVG(total_spend), 2)          AS avg_lifetime_spend
FROM (
    SELECT
        customer_id,
        COUNT(order_id)                 AS order_count,
        SUM(total_amount)               AS total_spend,
        CASE
            WHEN COUNT(order_id) = 1              THEN 'One-Time Buyer'
            WHEN COUNT(order_id) BETWEEN 2 AND 5  THEN 'Occasional Buyer'
            ELSE                                       'Loyal Buyer'
        END AS buyer_segment
    FROM orders
    GROUP BY customer_id
) customer_summary
GROUP BY buyer_segment
ORDER BY customer_count DESC;


-- ── Query 9: Discount Impact on Revenue ──────────────────────

SELECT
    payment_method,
    COUNT(order_id)                     AS order_count,
    ROUND(AVG(discount_pct), 1)         AS avg_discount_pct,
    ROUND(AVG(total_amount), 2)         AS avg_order_value,
    ROUND(SUM(total_amount), 2)         AS total_revenue
FROM orders
WHERE payment_method IS NOT NULL
GROUP BY payment_method
ORDER BY total_revenue DESC;
