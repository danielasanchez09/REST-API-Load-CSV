USE Solution

SELECT 
	ISNULL(Dep.name,'NOT ASSIGNED') AS Department,
	ISNULL(Jobs.name,'NOT ASSIGNED') AS Job,
	COUNT(CASE WHEN MONTH(Hired_Emp.hire_date) BETWEEN 1 AND 3 THEN Hired_Emp.id END) AS Q1,
	COUNT(CASE WHEN MONTH(Hired_Emp.hire_date) BETWEEN 4 AND 6 THEN Hired_Emp.id END) AS Q2,
	COUNT(CASE WHEN MONTH(Hired_Emp.hire_date) BETWEEN 7 AND 9 THEN Hired_Emp.id END) AS Q3,
	COUNT(CASE WHEN MONTH(Hired_Emp.hire_date) BETWEEN 10 AND 12 THEN Hired_Emp.id END) AS Q4
FROM hired_employees Hired_Emp
LEFT JOIN [dbo].[departments] Dep 
	ON Dep.id = Hired_Emp.department_id
LEFT JOIN [dbo].[jobs] Jobs
	ON Hired_Emp.job_id = Jobs.id
WHERE YEAR(Hired_Emp.hire_date) = 2021
GROUP BY Dep.name, Jobs.name
ORDER BY Dep.name asc, Jobs.name 

/*
SELECT *
FROM [dbo].[hired_employees]

SELECT *
FROM [dbo].[departments]

SELECT *
FROM [dbo].[jobs]
*/


