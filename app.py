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
    
if __name__ == '__main__':
    app.run(debug=True)