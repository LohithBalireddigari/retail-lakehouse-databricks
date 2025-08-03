-- Queries behind the Databricks SQL / AI-BI dashboard.
-- Replace workspace.retail with your catalog.schema if different.

-- 1. Headline KPIs
SELECT
  ROUND(SUM(revenue), 2)                         AS total_revenue,
  SUM(orders)                                    AS total_orders,
  ROUND(SUM(revenue) / SUM(orders), 2)           AS avg_order_value,
  SUM(units)                                     AS units_sold
FROM workspace.retail.gold_daily_revenue;

-- 2. Revenue trend with 7-day moving average
SELECT order_date, revenue, revenue_7d_avg
FROM workspace.retail.gold_daily_revenue
ORDER BY order_date;

-- 3. Monthly revenue and month-over-month growth
WITH monthly AS (
  SELECT DATE_TRUNC('month', order_date) AS month, SUM(revenue) AS revenue
  FROM workspace.retail.gold_daily_revenue
  GROUP BY 1
)
SELECT
  month,
  ROUND(revenue, 2) AS revenue,
  ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month) * 100, 1) AS mom_growth_pct
FROM monthly
ORDER BY month;

-- 4. Top 3 products in each category
SELECT category, product_name, brand, revenue, units_sold
FROM workspace.retail.gold_product_performance
WHERE rank_in_category <= 3
ORDER BY category, rank_in_category;

-- 5. Customer segments
SELECT segment,
       COUNT(*)                          AS customers,
       ROUND(AVG(lifetime_revenue), 2)   AS avg_ltv,
       ROUND(SUM(lifetime_revenue), 2)   AS segment_revenue
FROM workspace.retail.gold_customer_ltv
GROUP BY segment
ORDER BY segment_revenue DESC;

-- 6. Revenue by customer state
SELECT customer_state, ROUND(SUM(line_total), 2) AS revenue
FROM workspace.retail.gold_fact_sales
WHERE is_revenue
GROUP BY customer_state
ORDER BY revenue DESC;

-- 7. Channel mix by month
SELECT month, channel, revenue, revenue_share
FROM workspace.retail.gold_channel_mix
ORDER BY month, channel;

-- 8. Cancellation / return rate
SELECT status,
       COUNT(DISTINCT order_id) AS orders,
       ROUND(COUNT(DISTINCT order_id) * 100.0 / SUM(COUNT(DISTINCT order_id)) OVER (), 2) AS pct
FROM workspace.retail.gold_fact_sales
GROUP BY status
ORDER BY orders DESC;

-- 9. Latest data-quality run
SELECT entity, total, valid, quarantined, pass_rate, checked_at
FROM workspace.retail.gold_data_quality
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY checked_at DESC) = 1;
