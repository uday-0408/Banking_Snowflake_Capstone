{{ config(materialized='view', schema='gold') }}

SELECT 
    db.branch_name,
    SUM(CASE WHEN fc.complaint_priority = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'HIGH' THEN 1 ELSE 0 END) AS high_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_priority_complaints,
    SUM(CASE WHEN fc.complaint_priority = 'LOW' THEN 1 ELSE 0 END) AS low_priority_complaints
FROM {{ ref('fact_complaint') }} fc
JOIN {{ ref('dim_branch') }} db ON fc.branch_id = db.branch_id
GROUP BY db.branch_name
