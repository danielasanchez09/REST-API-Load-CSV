from flask import Flask, request, jsonify
import pandas as pd
import sqlalchemy 

#Hosting of API with Flask 
app = Flask(__name__)
"""if __name__ == '__main__':
    app.run(debug=True)"""
    

#Database configuration for SQL Server
Database_URL = 'mssql+pyodbc://@DESKTOP-OSC76TL\\SQLEXPRESS/Solution?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'

engine = sqlalchemy.create_engine(Database_URL)

metadata = sqlalchemy.MetaData()

#Define departments table
departments_table = sqlalchemy.Table(
    'departments', metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True), #id as a primary key column
    sqlalchemy.Column('name', sqlalchemy.String, nullable=False) #Department name, cannot be null

)

#Define jobs table
jobs_table = sqlalchemy.Table(
    'jobs', metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True), #id as a primary key
    sqlalchemy.Column('name', sqlalchemy.String, nullable=False) #Job Title cannot be null
)

#Define the Hired Employees table
employees_table = sqlalchemy.Table(
    'hired_employees', metadata,
    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True), #id as primary key
    sqlalchemy.Column('name', sqlalchemy.String, nullable=False), #Employee name cannot be null
    sqlalchemy.Column('hire_date', sqlalchemy.DateTime, nullable=False), #Date of hiring
    sqlalchemy.Column('department_id', sqlalchemy.Integer, sqlalchemy.ForeignKey('departments.id'), nullable=False), #Department id relation with departments
    sqlalchemy.Column('job_id', sqlalchemy.Integer, sqlalchemy.ForeignKey('jobs.id'), nullable=False) #Job id relation with jobs
)

#Create tables in the database
metadata.create_all(engine)

@app.route('/upload-csv', methods=['POST'])
def upload_csv():
    """Endpoint to upload CSV to the database"""

    file = request.files.get('file')
    table_name = request.form.get('table_name')

    if not file or not table_name:
        return jsonify({"error": "File and table_name are required."}), 400
    
    #Read the csv data file into a dataframe
    try: 
        df = pd.read_csv(file)
    except Exception as e:
        return jsonify({"error": f"Failed to read CSV file: {str(e)}"}), 400
    
    #Insert data into the specified table
    try:
        if 'id' in df.columns:
            df = df.drop(columns=['id']) #exclude de 'id' column
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        return jsonify({"message": "Data uploaded successfully."}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to upload data: {str(e)}"}), 500
    
@app.route('/batch-insert', methods=['POST'])
def batch_insert():
    """
    Endpoint tp insert data in batch transction
    expects a JSON payload with a table name and rows of data
    """

    data=request.get_json()
    table_name = data.get('table_name')
    rows = data.get('rows')

    if not table_name or not rows:
        return jsonify({"error": "Table name and rows are required"}), 400
    
    if len(rows) > 1000:
        return jsonify({"error": "Batch size cannot exceed 1000 rows"}), 400
    
    #Convert rows to a Dataframe
    try:
        df = pd.DataFrame(rows)
        
    except Exception as e:
        return jsonify({"error": f"Failed to process rows: {str(e)}"}), 400
    
    #insert data in tble 
    try:
            if 'id' not in df.columns:
                with engine.connect() as conn:
                    result = conn.execute(f"SELECT MAX(id) FROM {table_name}")
                    last_id = result.scalar() or 0 
                df['id'] = range(last_id + 1, last_id + 1 + len(df))
            else:
                df = df.drop(columns=['id'])

            df.to_sql(table_name, con=engine, if_exists='append', index=False)

            return jsonify({"message": "Batch inserted successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to insert batch: {str(e)}"}), 500

@app.route('/hires-by-quarter', methods=['GET'])
def hires_by_quarter():
    query = sqlalchemy.text("""
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
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query)
        data = [dict(row._mapping) for row in result]
    return jsonify(data)

@app.route('/greater-average-hires', methods=['GET'])
def greater_avg_hires():
    query = sqlalchemy.text("""
   
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
        
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        data =[dict(row._mapping) for row in result]
    return jsonify(data)
    

if __name__ == '__main__':
    app.run(debug=True)