--MRR
WITH monthly_revenue AS (
	SELECT 
		user_id,
		game_name,
		date_trunc('month', payment_date) AS month,
		sum(revenue_amount_usd ) AS monthly_revenue
	FROM games_payments
	GROUP BY 1,2,3 
),
monthly_all_revenue AS (
SELECT 
	mr.*,
	gpu.language,
    gpu.age,
    gpu.has_older_device_model
FROM monthly_revenue mr
LEFT JOIN games_paid_users gpu
ON mr.user_id = gpu.user_id
AND mr.game_name = gpu.game_name
)
SELECT
	user_id,
	game_name,
	month,
	monthly_revenue,
	language,
	age,
	has_older_device_model
FROM monthly_all_revenue
ORDER BY month;

--PAID USERS
WITH monthly_revenue AS (
	SELECT 
		user_id,
		game_name,
		date(date_trunc('month', payment_date)) AS month ,
		sum(revenue_amount_usd ) AS monthly_revenue
	FROM games_payments
	GROUP BY 1,2,3 
),
monthly_all_revenue AS (
SELECT 
	mr.*,
	gpu.language,
    gpu.age,
    gpu.has_older_device_model
FROM monthly_revenue mr
LEFT JOIN games_paid_users gpu
ON mr.user_id = gpu.user_id
)
SELECT
	count (DISTINCT user_id) AS paid_user,
	game_name,
	month,
	language,
	age,
	has_older_device_model
FROM monthly_all_revenue
GROUP BY 2,3,4,5,6
ORDER BY month;

--New Paid Users
WITH first_pay AS (
	SELECT  
		user_id,
		game_name,
		DATE(date_trunc('month', payment_date)) AS first_payment_month
	FROM games_payments gp
	GROUP BY user_id, game_name, payment_date
),
first_paid_month AS (
	SELECT
		user_id,
		game_name,
		MIN(first_payment_month) AS first_payment_month
	FROM first_pay
	GROUP BY user_id, game_name
)
SELECT
	count (DISTINCT (fpm.user_id)) AS new_paid_users,
	fpm.game_name,
	fpm.first_payment_month AS month,
	gpu.language,
	gpu.age,
	gpu.has_older_device_model
FROM first_paid_month fpm
LEFT JOIN games_paid_users gpu
ON fpm.user_id = gpu.user_id
GROUP BY 2,3,4,5,6
ORDER BY month;

-- New MRR
WITH payments_with_users AS (
    SELECT
        gp.user_id,
        gp.game_name,
        gp.payment_date,
        date (DATE_TRUNC('month', gp.payment_date)) AS month,
        gp.revenue_amount_usd,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM games_payments gp
    LEFT JOIN games_paid_users gpu
        ON gp.user_id = gpu.user_id
       AND gp.game_name = gpu.game_name
),
first_payment AS (
    SELECT
        user_id,
        game_name,
        MIN(month) AS first_payment_month
    FROM payments_with_users
    GROUP BY 1,2
)
SELECT
	pwu.user_id,
    pwu.month,
    SUM(pwu.revenue_amount_usd) AS new_mrr,
    pwu.language,
    pwu.age
FROM payments_with_users pwu
JOIN first_payment fp
    ON pwu.user_id = fp.user_id
   AND pwu.month = fp.first_payment_month
GROUP BY 1,2,4,5
ORDER BY 2;

--Expansion MRR
WITH monthly_revenue AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS month,
        SUM(gp.revenue_amount_usd) AS revenue
    FROM games_payments gp
    GROUP BY gp.user_id, gp.game_name, month
),
revenue_with_prev AS (
    SELECT
        curr.user_id,
        curr.game_name,
        curr.month,
        curr.revenue,
        prev.revenue AS prev_revenue
    FROM monthly_revenue curr
    JOIN monthly_revenue prev
      ON curr.user_id = prev.user_id
     AND curr.month = prev.month + INTERVAL '1 month'
)
SELECT
    rwp.user_id,
    rwp.game_name,
    rwp.month,
    (rwp.revenue - rwp.prev_revenue) AS expansion_mrr,
    gpu.language,
    gpu.age,
    gpu.has_older_device_model
FROM revenue_with_prev rwp
LEFT JOIN games_paid_users gpu
  ON rwp.user_id = gpu.user_id
WHERE rwp.revenue > rwp.prev_revenue
ORDER BY rwp.month;


--Contraction MRR
WITH monthly_revenue AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS month,
        SUM(gp.revenue_amount_usd) AS revenue
    FROM games_payments gp
    GROUP BY gp.user_id, gp.game_name, month
),
revenue_with_prev AS (
    SELECT
        curr.user_id,
        curr.game_name,
        curr.month,
        curr.revenue,
        prev.revenue AS prev_revenue
    FROM monthly_revenue curr
    JOIN monthly_revenue prev
      ON curr.user_id = prev.user_id
     AND curr.game_name = prev.game_name
     AND curr.month = prev.month + INTERVAL '1 month'
)
SELECT
    rwp.user_id,
    rwp.game_name,
    rwp.month,
    (rwp.prev_revenue - rwp.revenue) AS contraction_mrr,
    gpu.language,
    gpu.age,
    gpu.has_older_device_model
FROM revenue_with_prev rwp
LEFT JOIN games_paid_users gpu
  ON rwp.user_id = gpu.user_id
 AND rwp.game_name = gpu.game_name
WHERE rwp.revenue < rwp.prev_revenue
ORDER BY rwp.month;


--Churned Users
WITH payments_with_users AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS month,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM games_payments gp
    LEFT JOIN games_paid_users gpu
        ON gp.user_id = gpu.user_id
       AND gp.game_name = gpu.game_name
    GROUP BY 1,2,3,4,5,6
),
user_month_sequence AS (
    SELECT
        *,
        LEAD(month) OVER (
            PARTITION BY user_id, game_name
            ORDER BY month
        ) AS next_month
    FROM payments_with_users
)
SELECT
    COUNT(DISTINCT user_id) AS churned_users,
    game_name,
    (month + INTERVAL '1 month') AS churn_month,
    language,
    age,
    has_older_device_model
FROM user_month_sequence
WHERE next_month IS NULL
GROUP BY 2,3,4,5,6
ORDER BY churn_month;

--Churned Revenue
WITH monthly_revenue AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS month,
        SUM(gp.revenue_amount_usd) AS revenue
    FROM games_payments gp
    GROUP BY gp.user_id, gp.game_name, month
),
revenue_with_users AS (
    SELECT
        mr.*,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM monthly_revenue mr
    LEFT JOIN games_paid_users gpu
      ON mr.user_id = gpu.user_id
     AND mr.game_name = gpu.game_name
),
churned_revenue_base AS (
    SELECT
        prev.user_id,
        prev.game_name,
        (prev.month + INTERVAL '1 month') AS churn_month,
        prev.revenue AS churned_revenue,
        prev.language,
        prev.age,
        prev.has_older_device_model
    FROM revenue_with_users prev
    LEFT JOIN revenue_with_users curr
      ON prev.user_id = curr.user_id
     AND prev.game_name = curr.game_name
     AND curr.month = prev.month + INTERVAL '1 month'
    WHERE curr.user_id IS NULL
)
SELECT *
FROM churned_revenue_base
ORDER BY churn_month;


----Churn Rate
WITH user_months AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS month,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM games_payments gp
    LEFT JOIN games_paid_users gpu
      ON gp.user_id = gpu.user_id
     AND gp.game_name = gpu.game_name
    WHERE gp.revenue_amount_usd > 0
    GROUP BY 1,2,3,4,5,6
),
user_with_next AS (
    SELECT
        *,
        LEAD(month) OVER (
            PARTITION BY user_id, game_name
            ORDER BY month
        ) AS next_month
    FROM user_months
),
churned_users AS (
    SELECT
        (month + INTERVAL '1 month') AS churn_month,
        user_id,
        game_name,
        language,
        age,
        has_older_device_model
    FROM user_with_next
    WHERE next_month IS NULL
),
paid_users AS (
    SELECT
        month,
        COUNT(DISTINCT user_id) AS total_paid_users
    FROM user_months
    GROUP BY month
)
SELECT
    c.churn_month AS month,
    c.language,
    c.age,
    c.has_older_device_model,
    COUNT(DISTINCT c.user_id) AS churned_users,
    p.total_paid_users AS prev_month_paid_users,
    COUNT(DISTINCT c.user_id)::FLOAT / p.total_paid_users AS churn_rate
FROM churned_users c
JOIN paid_users p
  ON p.month = c.churn_month - INTERVAL '1 month'
GROUP BY 1,2,3,4,6
ORDER BY month;


-- Revenue Churn Rate 
WITH monthly_revenue AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date)) AS month,
        SUM(gp.revenue_amount_usd) AS revenue
    FROM games_payments gp
    GROUP BY gp.user_id, gp.game_name, month
),
revenue_with_users AS (
    SELECT
        mr.*,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM monthly_revenue mr
    LEFT JOIN games_paid_users gpu
      ON mr.user_id = gpu.user_id
     AND mr.game_name = gpu.game_name
),
revenue_with_next AS (
    SELECT
        *,
        LEAD(month) OVER (
            PARTITION BY user_id, game_name
            ORDER BY month
        ) AS next_month
    FROM revenue_with_users
),
churned_revenue AS (
    SELECT
        (month + INTERVAL '1 month') AS churn_month,
        user_id,
        game_name,
        revenue AS churned_revenue,
        language,
        age,
        has_older_device_model
    FROM revenue_with_next
    WHERE next_month IS NULL
),
monthly_total_revenue AS (
    SELECT
        month,
        SUM(revenue) AS total_revenue
    FROM monthly_revenue
    GROUP BY month
)
SELECT
    c.churn_month AS month,
    c.language,
    c.age,
    c.has_older_device_model,
    SUM(c.churned_revenue) AS churned_revenue,
    t.total_revenue AS prev_month_total_revenue,
    SUM(c.churned_revenue)::FLOAT / t.total_revenue AS revenue_churn_rate
FROM churned_revenue c
JOIN monthly_total_revenue t
  ON t.month = c.churn_month - INTERVAL '1 month'
GROUP BY 1,2,3,4,6
ORDER BY month;


--Customer Lifetime
WITH user_months AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date::DATE) AS month
    FROM games_payments
    WHERE revenue_amount_usd > 0
)
SELECT
    user_id,
    (
        DATE_PART('year', MAX(month)) - DATE_PART('year', MIN(month))
    ) * 12
    + (
        DATE_PART('month', MAX(month)) - DATE_PART('month', MIN(month))
    ) + 1 AS customer_lifetime_months
FROM user_months
GROUP BY user_id;

--ARPPU
WITH monthly_revenue AS (
    SELECT
        gp.user_id,
        gp.game_name,
        DATE(DATE_TRUNC('month', gp.payment_date :: DATE)) AS month,
        SUM(gp.revenue_amount_usd) AS monthly_revenue
    FROM games_payments gp
    GROUP BY 1,2,3
),
monthly_with_users AS (
    SELECT
        mr.user_id,
        mr.game_name,
        mr.month,
        mr.monthly_revenue,
        gpu.language,
        gpu.age,
        gpu.has_older_device_model
    FROM monthly_revenue mr
    LEFT JOIN games_paid_users gpu
        ON mr.user_id = gpu.user_id
)
SELECT
    month,
    language,
    age,
    has_older_device_model,
           SUM(monthly_revenue)
        / COUNT(DISTINCT CASE WHEN monthly_revenue > 0 THEN user_id END) AS ARPPU
FROM monthly_with_users
GROUP BY 1,2,3,4
ORDER BY 1;

