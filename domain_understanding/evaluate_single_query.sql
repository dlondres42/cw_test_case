-- SQL equivalent of PolicyAnomalyDetector.evaluate_single(...)
--
-- This query mirrors the Python logic from monitoring_service/app/detector.py:
--   1) If history length < min_history:
--      - problem status + count > 0 => CRITICAL, anomalous (z_score=10.0)
--      - otherwise => NORMAL, not anomalous
--   2) Else compute rolling mean/std for the selected status and evaluate z-score:
--      z = (count - mean) / max(std, 1.0)
--      WARNING if z > z_score_threshold, CRITICAL if z > critical_threshold.
--
-- Expected input table (one row per minute): history_window
--   columns: denied, failed, reversed, backend_reversed, approved
--
-- Edit the values in input_params for each evaluation call.

WITH
input_params AS (
    SELECT
        -- request payload
        'denied' AS status,
        5571     AS observed_count,

        -- detector config
        30       AS min_history,
        2.5      AS z_score_threshold,
        4.0      AS critical_threshold
),
problem_statuses AS (
    SELECT 'denied' AS status
    UNION ALL SELECT 'failed'
    UNION ALL SELECT 'reversed'
    UNION ALL SELECT 'backend_reversed'
),
history_values AS (
    SELECT
        CASE p.status
            WHEN 'denied' THEN CAST(COALESCE(h.denied, 0) AS REAL)
            WHEN 'failed' THEN CAST(COALESCE(h.failed, 0) AS REAL)
            WHEN 'reversed' THEN CAST(COALESCE(h.reversed, 0) AS REAL)
            WHEN 'backend_reversed' THEN CAST(COALESCE(h.backend_reversed, 0) AS REAL)
            WHEN 'approved' THEN CAST(COALESCE(h.approved, 0) AS REAL)
            ELSE 0.0
        END AS value
    FROM history_window h
    CROSS JOIN input_params p
),
rollup AS (
    SELECT
        COUNT(*) AS n,
        COALESCE(AVG(value), 0.0) AS mean
    FROM history_values
),
std_calc AS (
    SELECT
        r.n,
        r.mean,
        CASE
            WHEN r.n < 2 THEN 0.0
            ELSE sqrt(SUM((hv.value - r.mean) * (hv.value - r.mean)) / (r.n - 1))
        END AS std
    FROM rollup r
    LEFT JOIN history_values hv ON 1 = 1
),
scored AS (
    SELECT
        p.status,
        p.observed_count,
        s.n,
        s.mean,
        s.std,
        (CAST(p.observed_count AS REAL) - s.mean) /
            CASE WHEN s.std > 1.0 THEN s.std ELSE 1.0 END AS z_score,
        EXISTS (
            SELECT 1
            FROM problem_statuses ps
            WHERE ps.status = p.status
        ) AS is_problem_status,
        p.min_history,
        p.z_score_threshold,
        p.critical_threshold
    FROM input_params p
    CROSS JOIN std_calc s
)
SELECT
    status,
    CASE
        WHEN n < min_history AND is_problem_status = 1 AND observed_count > 0 THEN 'CRITICAL'
        WHEN n < min_history THEN 'NORMAL'
        WHEN z_score > critical_threshold THEN 'CRITICAL'
        WHEN z_score > z_score_threshold THEN 'WARNING'
        ELSE 'NORMAL'
    END AS severity,

    ROUND(
        CASE
            WHEN n < min_history AND is_problem_status = 1 AND observed_count > 0 THEN 10.0
            WHEN n < min_history THEN 0.0
            ELSE z_score
        END,
        2
    ) AS z_score,

    ROUND(
        CASE
            WHEN n < min_history THEN 0.0
            ELSE mean
        END,
        2
    ) AS baseline_mean,

    ROUND(
        CASE
            WHEN n < min_history THEN 0.0
            ELSE std
        END,
        2
    ) AS baseline_std,

    CASE
        WHEN n < min_history AND is_problem_status = 1 AND observed_count > 0 THEN 1
        WHEN n < min_history THEN 0
        WHEN z_score > z_score_threshold THEN 1
        ELSE 0
    END AS is_anomalous,

    CASE
        WHEN n < min_history AND is_problem_status = 1 AND observed_count > 0 THEN
            status || ' count ' || observed_count ||
            ' detected with no historical baseline (problem status should be rare/zero)'

        WHEN n < min_history THEN
            'Insufficient history (' || n || ' < ' || min_history || ') for reliable evaluation.'

        WHEN z_score > z_score_threshold THEN
            status || ' count ' || observed_count ||
            ' is ' || printf('%.1f', z_score) || 'σ above baseline (mean=' ||
            printf('%.1f', mean) || ', std=' || printf('%.1f', std) || ')'

        ELSE ''
    END AS message
FROM scored;
