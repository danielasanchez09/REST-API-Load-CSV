USE Solution;

/*
SELECT *
FROM [dbo].[hired_employees]

SELECT *
FROM [dbo].[departments]

SELECT *
FROM [dbo].[jobs]
*/


WITH HiresByDepartment AS (
	
	SELECT 
		ISNULL(Dep.name,'Not Assigned') AS Department,
		Dep.id,
		COUNT(*) AS TotalHires
	FROM [dbo].[hired_employees] Hired_Emp
	LEFT JOIN [dbo].[departments] Dep
		ON Hired_Emp.department_id = Dep.id
	WHERE YEAR(Hired_Emp.hire_date) = 2021
	GROUP BY Dep.name, Dep.id

)

SELECT
	DepAvgHires.id,
	DepAvgHires.Department,
	DepAvgHires.TotalHires
	--,DepAvgHires.MeanTotalHires
FROM (
	
	SELECT 
		id,
		department,
		TotalHires,
		AVG(TotalHires) OVER () AS MeanTotalHires
	FROM HiresByDepartment


) DepAvgHires
WHERE DepAvgHires.TotalHires > DepAvgHires.MeanTotalHires
ORDER BY DepAvgHires.TotalHires DESC 
	
