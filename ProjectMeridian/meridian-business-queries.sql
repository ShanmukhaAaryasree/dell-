-- Run in Databricks SQL after the batch notebook has created the Delta tables.
-- Change aarya.default if the notebook uses a different output schema.

-- Reconcile curated shipment volumes by status, including cancellations and returns.
SELECT status, COUNT(*) AS shipments
FROM aarya.default.meridian_shipments_curated
GROUP BY status
ORDER BY status;

-- Measure freight revenue by shipment month and customer segment.
WITH customers AS (
  SELECT customer_id, segment
  FROM aarya.default.meridian_customers_raw
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRIM(customer_id)
    ORDER BY TRY_CAST(signup_date AS DATE) ASC NULLS LAST,
             TO_JSON(STRUCT(customer_id, customer_name, email, city, segment, signup_date))
  ) = 1
)
SELECT
  TRUNC(s.ship_dt, 'MONTH') AS shipment_month,
  c.segment AS customer_segment,
  COUNT(*) AS shipment_volume,
  SUM(s.charge) AS freight_revenue_inr
FROM aarya.default.meridian_shipments_curated AS s
JOIN customers AS c ON TRIM(s.customer_id) = TRIM(c.customer_id)
WHERE s.status <> 'CANCELLED'
GROUP BY TRUNC(s.ship_dt, 'MONTH'), c.segment
ORDER BY shipment_month, customer_segment;

-- Compare the delay-reason mix across operational seasons.
WITH seasonal_delays AS (
  SELECT
    CASE
      WHEN MONTH(shipment_month) IN (1, 2) THEN 'Jan–Feb'
      WHEN MONTH(shipment_month) IN (3, 4, 5) THEN 'Mar–May'
      WHEN MONTH(shipment_month) BETWEEN 6 AND 9 THEN 'Jun–Sep'
      ELSE 'Oct–Dec'
    END AS season,
    delay_reason,
    late_shipments
  FROM aarya.default.meridian_kpi_delay_reasons
), reason_totals AS (
  SELECT season, delay_reason, SUM(late_shipments) AS late_shipments
  FROM seasonal_delays
  GROUP BY season, delay_reason
)
SELECT season, delay_reason, late_shipments,
       ROUND(100.0 * late_shipments / SUM(late_shipments) OVER (PARTITION BY season), 2)
         AS share_of_late_shipments_pct
FROM reason_totals
ORDER BY season, late_shipments DESC, delay_reason;

-- Review rejected shipments and the validation rules they triggered.
SELECT shipment_id, shipment_status, last_updated, rejection_reason
FROM aarya.default.meridian_shipments_rejected
ORDER BY shipment_id, last_updated;
