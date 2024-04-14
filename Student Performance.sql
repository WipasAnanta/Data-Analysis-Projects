-- look at the dataset
SELECT *
FROM StudentsPerformance;

-- find the top performance students
SELECT
	gender,
    math_score + reading_score + writing_score AS total_score
FROM StudentsPerformance
ORDER BY 2 DESC
LIMIT 10

-- students performance is above or below average
-- first, find average score
SELECT AVG(math_score + reading_score + writing_score) AS avg_score
FROM StudentsPerformance -- avg score = 203.312

WITH total_score AS (
  SELECT
		gender,
    	math_score + reading_score + writing_score as total_score
	FROM StudentsPerformance
)
SELECT 
	gender,
	total_score,
CASE
   	WHEN total_score > 203.312 THEN 'yes'
   	ELSE 'no'
END AS avg_or_not
FROM total_score

-- which gender get the highest math score
SELECT
	gender,
    math_score
FROM StudentsPerformance
ORDER BY 2 DESC
LIMIT 10

-- is parental level of education affect children performance?
SELECT
	parental_level_of_education,
    math_score + reading_score + writing_score AS total_score
FROM StudentsPerformance
GROUP BY 1
ORDER BY 2 DESC

-- number of students in each lunch program
SELECT 
	lunch,
    COUNT(*) AS number_of_students
FROM StudentsPerformance
GROUP BY 1

-- number of students attending the school by each parental education level
SELECT
	parental_level_of_education,
    count(*) AS number_of_students
FROM StudentsPerformance
GROUP BY 1
ORDER BY 2