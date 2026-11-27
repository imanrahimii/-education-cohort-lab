"""SQL shared by the Databricks notebook and portable CI checks."""

VALID_STUDENT = """
student_id IS NOT NULL AND TRIM(student_id) <> ''
AND cohort IS NOT NULL AND cohort IN ('Domestic', 'International')
AND age_band IS NOT NULL AND age_band IN ('Under 25', '25+')
AND field IS NOT NULL AND TRIM(field) <> ''
AND (continued IS NULL OR continued IN ('', '0', '1'))
AND revision IS NOT NULL AND revision IN ('1', '2')
"""

REJECTED = f"SELECT * FROM {{bronze_students}} WHERE NOT ({VALID_STUDENT})"

STUDENTS = f"""
WITH dedup AS (
  SELECT DISTINCT student_id, cohort, age_band, field, continued, revision
  FROM {{bronze_students}}
  WHERE {VALID_STUDENT}
), ranked AS (
  SELECT *, DENSE_RANK() OVER (PARTITION BY student_id ORDER BY revision DESC) AS priority
  FROM dedup
), latest AS (
  SELECT *, COUNT(*) OVER (PARTITION BY student_id) AS candidates
  FROM ranked WHERE priority = 1
)
SELECT student_id, cohort, age_band, field,
       CAST(NULLIF(continued, '') AS INTEGER) AS continued
FROM latest WHERE candidates = 1
"""

SURVEY = """
SELECT r.student_id, r.outcome, MIN(CAST(r.result AS INTEGER)) AS result
FROM {bronze_survey} r
JOIN {silver_students} s ON r.student_id = s.student_id
WHERE r.outcome IN ('experience', 'employment') AND r.result IN ('0', '1')
GROUP BY r.student_id, r.outcome
HAVING COUNT(DISTINCT r.result) = 1
"""

METRICS = """
WITH outcomes AS (
  SELECT 'continuation' AS outcome
  UNION ALL SELECT 'experience'
  UNION ALL SELECT 'employment'
), measures AS (
  SELECT o.outcome, s.cohort, s.age_band,
         CASE WHEN o.outcome = 'continuation' THEN s.continued
              ELSE r.result END AS result
  FROM {silver_students} s CROSS JOIN outcomes o
  LEFT JOIN {silver_survey} r ON r.student_id = s.student_id AND r.outcome = o.outcome
)
SELECT outcome, cohort, age_band, COUNT(*) AS eligible, COUNT(result) AS observed,
       SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS positive,
       100.0 * SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END)
             / NULLIF(COUNT(result), 0) AS rate_pct
FROM measures GROUP BY outcome, cohort, age_band
"""
