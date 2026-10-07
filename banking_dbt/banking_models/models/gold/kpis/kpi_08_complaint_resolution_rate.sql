{{ config(materialized='view', schema='gold') }}

SELECT 
    db.branch_name,
    fc.complaint_category,
    COUNT(fc.complaint_id) AS total_complaints,
    SUM(CASE WHEN fc.complaint_status = 'RESOLVED' THEN 1 ELSE 0 END) AS resolved_complaints,
    ROUND((SUM(CASE WHEN fc.complaint_status = 'RESOLVED' THEN 1 ELSE 0 END) / COUNT(fc.complaint_id)) * 100, 2) AS resolution_rate
FROM {{ ref('fact_complaint') }} fc
JOIN {{ ref('dim_branch') }} db ON fc.branch_id = db.branch_id
GROUP BY db.branch_name, fc.complaint_category
