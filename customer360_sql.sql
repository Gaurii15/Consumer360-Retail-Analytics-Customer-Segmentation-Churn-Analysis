CREATE TABLE raw_sales (
    order_id        VARCHAR(50),
    order_date      DATE,
    customer_id     VARCHAR(50),
    customer_name   VARCHAR(100),
    product_id      VARCHAR(50),
    product_name    VARCHAR(150),
    category        VARCHAR(100),
	brand			VARCHAR(100),
    quantity        INT,
    unit_price      DECIMAL(10,2),
	discount		DECIMAL(10,2),
	tax				DECIMAL (10,2),
	ShippingCost	DECIMAL (10,2),
    total_amount    DECIMAL(10,2),
	payment_mode	VARCHAR(50),
	order_status	VARCHAR(50),
    country         VARCHAR(50),
    state           VARCHAR(50),
    city            VARCHAR(50),
	seller_id		VARCHAR(50)
);

SELECT *FROM raw_sales;

--Create Clean Table
CREATE TABLE cleaned_sales AS
SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    country,
    order_status
FROM raw_sales
WHERE customer_id IS NOT NULL
  AND quantity > 0
  AND unit_price > 0
  AND order_status = 'Delivered';

SELECT *FROM cleaned_sales;

--Add Revenue Column
ALTER TABLE cleaned_sales
ADD COLUMN revenue DECIMAL(10,2);

--Populate Revenue Column
UPDATE cleaned_sales
SET revenue = quantity * unit_price;

--Create fact table
CREATE TABLE fact_sales AS
SELECT
    order_id,
    order_date,
    customer_id,
    product_id,
    quantity,
    revenue
FROM cleaned_sales;

SELECT *FROM fact_sales;

--Customer Dimension table
CREATE TABLE dim_customer AS
SELECT DISTINCT
    customer_id,
    country
FROM cleaned_sales;

SELECT *FROM dim_customer;

--Product Dimension
CREATE TABLE dim_product AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    unit_price
FROM cleaned_sales;

SELECT *FROM dim_product;

CREATE VIEW single_customer_view AS
SELECT
    customer_id,
    MAX(order_date) AS last_purchase_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(revenue) AS total_revenue,
    country
FROM cleaned_sales
GROUP BY customer_id, country;

SELECT *FROM single_customer_view;

--Top customers by revenue
SELECT product_id, SUM(revenue) AS TotalRevenue
FROM fact_sales
GROUP BY product_id
ORDER BY TotalRevenue DESC
LIMIT 10;

--monthly sales trend
SELECT 
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(revenue) AS total_sales
FROM fact_sales
GROUP BY 
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date)
ORDER BY year, month;

--RFM analysis
WITH RFM AS (
    SELECT 
        customer_id,
        -- Recency: days since last purchase
        (CURRENT_DATE - MAX(order_date)) AS recency,
        -- Frequency: number of orders
        COUNT(DISTINCT order_id) AS frequency,
        -- Monetary: total spent
        SUM(revenue) AS monetary
    FROM fact_sales
    GROUP BY customer_id
)
SELECT *
FROM RFM
ORDER BY recency;


--Customer churn analysis
WITH RFM AS (
    SELECT 
        customer_id,
        (CURRENT_DATE - MAX(order_date)) AS recency,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(revenue) AS monetary
    FROM fact_sales
    GROUP BY customer_id
)
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    CASE 
        WHEN recency > 180 THEN 'Churned'
        ELSE 'Active'
    END AS status
FROM RFM
ORDER BY recency DESC;


