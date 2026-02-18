-- 3-layer statistical anomaly detection query (SQL version)
--
-- Expected input table: checkout_data
-- Required columns:
--   time, today, yesterday, same_day_last_week, avg_last_week, avg_last_month
--
-- Notes:
-- - This version intentionally implements the 3 layers used in the notebooks:
--   (1) Statistical Z-Score
--   (2) Volume-aware rules
--   (3) Drop-to-zero / spike detection
-- - The optional slope/rate-of-change layer is not included.

WITH
params AS (
    SELECT
        2.0  AS z_threshold,
        3.0  AS spike_multiplier,
        5.0  AS min_volume_for_outage
),
base AS (
    SELECT
        c.time,
        CAST(REPLACE(c.time, 'h', '') AS INTEGER) AS hour,
        c.today,
        c.yesterday,
        c.same_day_last_week,
        c.avg_last_week,
        c.avg_last_month
    FROM checkout_data c
),
layer1 AS (
    SELECT
        b.*,
        -- Weighted expected value
        (
            0.30 * b.avg_last_week +
            0.25 * b.avg_last_month +
            0.25 * b.same_day_last_week +
            0.20 * b.yesterday
        ) AS expected,

        -- Sample std over the 4 reference values (n-1 = 3)
        (
            (b.yesterday + b.same_day_last_week + b.avg_last_week + b.avg_last_month) / 4.0
        ) AS ref_mean
    FROM base b
),
layer1_std AS (
    SELECT
        l1.*,
        sqrt(
            (
                ((l1.yesterday         - l1.ref_mean) * (l1.yesterday         - l1.ref_mean)) +
                ((l1.same_day_last_week - l1.ref_mean) * (l1.same_day_last_week - l1.ref_mean)) +
                ((l1.avg_last_week     - l1.ref_mean) * (l1.avg_last_week     - l1.ref_mean)) +
                ((l1.avg_last_month    - l1.ref_mean) * (l1.avg_last_month    - l1.ref_mean))
            ) / 3.0
        ) AS std_raw
    FROM layer1 l1
),
layer1_scored AS (
    SELECT
        l1s.*,
        CASE
            WHEN l1s.std_raw >= 1.0 AND l1s.std_raw >= (0.3 * l1s.expected) THEN l1s.std_raw
            WHEN 1.0 >= (0.3 * l1s.expected) THEN 1.0
            ELSE (0.3 * l1s.expected)
        END AS estimated_std
    FROM layer1_std l1s
),
layer2 AS (
    SELECT
        l1.*,
        (l1.today - l1.expected) / NULLIF(l1.estimated_std, 0.0) AS z_score,
        CASE
            WHEN ABS((l1.today - l1.expected) / NULLIF(l1.estimated_std, 0.0)) > p.z_threshold THEN 1
            ELSE 0
        END AS is_zscore_anomaly,

        CASE WHEN l1.avg_last_month < 5 THEN 1 ELSE 0 END AS is_low_traffic,
        ABS(l1.today - l1.expected) AS abs_deviation,
        ABS((l1.today - l1.expected) / NULLIF(l1.expected, 0.0)) AS pct_deviation,

        CASE
            -- Low traffic rule
            WHEN l1.avg_last_month < 5
                 AND ABS(l1.today - l1.expected) > 10
                 AND ABS((l1.today - l1.expected) / NULLIF(l1.estimated_std, 0.0)) > p.z_threshold
            THEN 1

            -- High traffic rule
            WHEN l1.avg_last_month >= 5
                 AND ABS((l1.today - l1.expected) / NULLIF(l1.expected, 0.0)) > 1.0
                 AND ABS((l1.today - l1.expected) / NULLIF(l1.estimated_std, 0.0)) > (p.z_threshold * 0.8)
            THEN 1

            ELSE 0
        END AS is_volume_anomaly
    FROM layer1_scored l1
    CROSS JOIN params p
),
layer3 AS (
    SELECT
        l2.*,
        MAX(l2.yesterday, l2.same_day_last_week, l2.avg_last_week, l2.avg_last_month) AS historical_max,

        CASE
            WHEN l2.today = 0 AND l2.expected >= p.min_volume_for_outage THEN 1
            ELSE 0
        END AS is_outage,

        CASE
            WHEN l2.today > (p.spike_multiplier * MAX(l2.yesterday, l2.same_day_last_week, l2.avg_last_week, l2.avg_last_month))
                 AND l2.today > 10
            THEN 1
            ELSE 0
        END AS is_spike
    FROM layer2 l2
    CROSS JOIN params p
),
scored AS (
    SELECT
        l3.*,
        (
            l3.is_zscore_anomaly +
            l3.is_volume_anomaly +
            (2 * l3.is_outage) +
            (2 * l3.is_spike)
        ) AS anomaly_score
    FROM layer3 l3
)
SELECT
    s.time,
    s.hour,
    s.today,
    s.yesterday,
    s.same_day_last_week,
    s.avg_last_week,
    s.avg_last_month,

    s.expected,
    s.estimated_std,
    s.z_score,

    s.is_zscore_anomaly,
    s.is_low_traffic,
    s.abs_deviation,
    s.pct_deviation,
    s.is_volume_anomaly,

    s.is_outage,
    s.historical_max,
    s.is_spike,

    s.anomaly_score,

    CASE
        WHEN s.is_outage = 1 THEN 'CRITICAL'
        WHEN s.is_spike  = 1 THEN 'WARNING'
        WHEN s.anomaly_score >= 2 THEN 'WARNING'
        WHEN s.anomaly_score = 1 THEN 'INFO'
        ELSE 'NORMAL'
    END AS severity,

    CASE WHEN s.anomaly_score > 0 THEN 1 ELSE 0 END AS is_anomalous
FROM scored s
ORDER BY s.hour;
