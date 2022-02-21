# sql_repository
In this repository I store my SQL_code

I see technology as means to achieve business value. 

## Bestsellers, solidus

SELECT sku, color, sum(quantity) AS sumcount FROM spree_orders o 
LEFT JOIN spree_line_items i ON o.id = i.order_id
LEFT JOIN spree_variants v ON v.id = i.variant_id
WHERE completed_at > '01-01-2019' AND completed_at < '01-01-2020'
GROUP BY sku, color, quantity ORDER BY sumcount DESC

## odoo, compute quantities

# moves in
SELECT
  product_id,
  SUM(quantity)
FROM
  stock_move AS sm
WHERE
  product_id IN ()
  AND location_dest_id IN (<FUNCTION TO GET LOCATION IDs>)
  AND state IN ('waiting', 'confirmed', 'assigned', 'partially_available')
  AND date >= ""
GROUP BY
  product_id

# moves out
SELECT
  product_id,
  SUM(quantity)
FROM
  stock_move AS sm
WHERE
  product_id IN ()
  AND location_dest_id IN ()
  AND state IN ('waiting', 'confirmed', 'assigned', 'partially_available')
  AND date >= ""
GROUP BY
  product_id

# quantity
SELECT
  product_id,
  SUM(quantity)
FROM
  stock_quant AS sq
WHERE
  product_id IN ()
  AND location_id IN ()
  AND state IN ('waiting', 'confirmed', 'assigned', 'partially_available')
  AND date >= ""
GROUP BY
  product_id
  
  
  ## odoo, stock count differences 
  
  SELECT
    sm.name,
    slFrom.complete_name AS location_from,
    sl.complete_name AS location_to,
    sm.date,
    p.internal_description AS product,
    sm.product_qty
FROM
  stock_move AS sm
JOIN
  stock_location AS sl
ON sm.location_dest_id = sl.id
JOIN
    product_product AS p
ON sm.product_id = p.id
JOIN
    stock_location AS slFrom
ON sm.location_id = slFRom.id
WHERE
    (    
        sl.complete_name = 'NLAMS01/Available for sales'
    OR
        slFrom.complete_name = 'NLAMS01/Available for sales'
    )
AND
    p.internal_description = 'neil-satin-gold-demo'
ORDER BY
    DATE DESC
  
  ## Active customers from solidus 
  
  WITH order_pre_processing AS (
    SELECT
        *,
        o.created_at AS order_date,
        o.id AS id_spree_orders,
        CONCAT(
            channel,
            '-',
            payment_state,
            '-',
            CASE WHEN LOWER(s.overall_status) IN (
                'shipped',
                'sent',
                'shipped_to_store',
                'complete',
                'production',
                'processing',
                'info_incomplete',
                'delivered'
            ) THEN 'other' ELSE 'new' END
        ) AS statuses_grouped
    FROM
        dl_solidus.spree_orders o
        LEFT JOIN dl_solidus.spree_order_statuses s ON o.id = s.order_id
    WHERE
        number NOT LIKE '%-T'
        AND number NOT LIKE '%-E'
        AND email NOT LIKE '%@aceandate.com'
),

order_sequence AS (


SELECT 
   order_date,
   email,
   number,
  

   ROW_NUMBER() OVER (PARTITION BY email ORDER BY 
   order_date ASC) AS customer_order_sequence,

   LAG(order_date) OVER (PARTITION BY email ORDER BY 
   order_date ASC) AS previous_order_date

   FROM order_pre_processing 
   WHERE order_date > '01-01-2017' 
        
   GROUP BY order_date, email ,number),

time_between_orders AS (SELECT 
   order_date,
   email,
   customer_order_sequence,
   number,
   

   CASE WHEN previous_order_date IS NULL THEN order_date
   ELSE previous_order_date END AS previous_order_date,

 
   EXTRACT(DAY FROM order_date - previous_order_date) AS  days_between_orders

   FROM order_sequence), 

customer_life_cycle AS (
    SELECT 
   order_date,
   email,
    number,
    

   CASE 
   WHEN customer_order_sequence = 1 THEN 'NEW Customer'
   WHEN days_between_orders > 0 AND days_between_orders < 366 
   THEN 'Active Customer'
   WHEN days_between_orders > 365 THEN 'Lapsed Customer'
   ELSE 'Unknown' 
   END AS customer_life_cycle,

   customer_order_sequence,
   previous_order_date,

   CASE 
   WHEN days_between_orders IS NULL THEN 0
   ELSE days_between_orders 
   END AS days_between_orders

   FROM time_between_orders)

SELECT * FROM customer_life_cycle
  
  
  ## Latest stock record solidus
  
  SELECT s.variant_id, s.created_at, s.count_on_hand, stock_location_id
FROM spree_stock_logs s
INNER JOIN (
    SELECT variant_id, max(created_at) AS MaxDate
    FROM spree_stock_logs
    GROUP BY variant_id
) sm ON s.variant_id = sm.variant_id AND s.created_at = sm.MaxDate WHERE stock_location_id = 1
  
 ## window functions
  
  The trick here is to realize that you need to compare an individual value (the revenue from a particular line item) with an aggregated value
 (the sum of all line item revenue in a particular state). Any time you need to do something like this a window function is a good bet. 
 Here’s how that query might look:
with state_totals as (
  SELECT state, revenue, 
    SUM(revenue) OVER (PARTITION BY state) as state_revenue
  FROM state_line_items)
SELECT state, 
  revenue/state_revenue as percent_of_state_revenue
FROM state_totals;
  
## An ETL script to see if test orders converted to a real purchase 
  
  DROP TABLE IF EXISTS ol.sp_orders;

CREATE TABLE ol.sp_orders (
    number                         TEXT,
    email                          TEXT,
    completed_at                   TIMESTAMP,
    next_order_date                TIMESTAMP,
    preceding_order_date           TIMESTAMP,
    hto_conversion_id              TEXT,
    hto_conversion                 BOOLEAN,
    hto_conversion_days            INTERVAL,
    paid_order_was_preceded_by_hto BOOLEAN,
    conversion_days                INTERVAL
);

WITH order_pre_processing AS (
    SELECT *,
           CONCAT(channel, '-', payment_state, '-', CASE
                                                        WHEN LOWER(s.overall_status) IN
                                                             ('shipped', 'sent', 'shipped_to_store', 'complete',
                                                              'production', 'processing', 'info_incomplete',
                                                              'delivered') THEN 'other'
                                                        ELSE 'new' END) AS statuses_grouped
    FROM dl_solidus.spree_orders o
             LEFT JOIN dl_solidus.spree_order_statuses s
                       ON o.id = s.order_id
    WHERE completed_at IS NOT NULL
      AND number NOT LIKE '%-E'
),
     valid_orders AS (
         SELECT *
         FROM order_pre_processing
         WHERE statuses_grouped IN
               ('spree-balance_due-other', 'spree-credit_owed-other', 'spree-credit_owed-new', 'spree-paid-new',
                'spree-paid-other', 'spree-refunded-new', 'spree-refunded-other')
     ),
     base AS (
         SELECT number,
                email,
                completed_at,
                number LIKE '%-T'     AS hto_order,
                number NOT LIKE '%-T' AS paid_order
         FROM valid_orders
     ),
     lead_base AS
         (SELECT number,
                 email,
                 completed_at,
                 hto_order,
                 paid_order,
                 LEAD(completed_at)
                 OVER (
                     PARTITION BY email
                     ORDER BY completed_at )            AS next_order_date,
                 LAG(completed_at)
                 OVER (
                     PARTITION BY email
                     ORDER BY completed_at )            AS preceding_order_date,
                 LEAD(number)
                 OVER (
                     PARTITION BY email
                     ORDER BY completed_at )            AS lead_invoice_id,
                 LAG(number)
                 OVER (
                     PARTITION BY email
                     ORDER BY completed_at )            AS lag_invoice_id,
                 LEAD(hto_order)
                 OVER (
                     PARTITION BY email
                     ORDER BY completed_at ) :: BOOLEAN AS lead_hto_order,
                 LAG(paid_order)
                 OVER (
                     PARTITION BY email
                     ORDER BY completed_at ) :: BOOLEAN AS lag_paid_order

          FROM base
          ORDER BY 1, 2
         )

INSERT
INTO ol.sp_orders (number, email, completed_at, next_order_date, preceding_order_date, hto_conversion_id,
                   hto_conversion, hto_conversion_days, paid_order_was_preceded_by_hto, conversion_days)

SELECT number,
       email,
       completed_at,
       next_order_date,
       preceding_order_date,
       CASE WHEN hto_order THEN lead_invoice_id END                                               AS hto_conversion_id,
       (hto_order AND NOT lead_hto_order)                                                         AS hto_conversion,
       CASE WHEN (hto_order AND NOT lead_hto_order)
               THEN next_order_date - completed_at END                                            as hto_conversion_days,
       (paid_order AND NOT lag_paid_order)                                                        AS paid_order_was_preceded_by_hto,
       CASE WHEN (paid_order AND NOT lag_paid_order) THEN completed_at - preceding_order_date END as conversion_days
FROM lead_base
ORDER BY 2, 3
;
  
## window function first value, first name of car 
  
 SELECT 
   FIRST_VALUE(name) OVER(PARTITION BY model, year ORDER BY date_at_lot ASC) AS oldest_car_name
   model,
   year
FROM cars
  
Doing this will result in the function outputting the name of the first car. You can specify any column here, but the name makes the most sense since it is unique across the model and year. 

