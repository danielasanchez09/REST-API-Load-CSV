#SQL Data Base Migration REST API

This API with FLASK application provides a REST API to migrate data from CSV files to a SQL Server database. It does batch inserts 

#Features
- Upload CSV files and map themm to database tables
- Makes batch inserts of up to 1000 rows
- Auto handle 'id' fields (incremental or db managed)

#Requirements
- Python 3.x
- SQL Server Express (or any SQL instance)
- Flask
- Sqlalchemy
- Pandas
- Pyodbc

#Intalation

1. Clone repository: https://github.com/danielasanchez09/REST-API-Load-CSV

2. Install dependencies:
    pip install -r requirements.txt

3. Update the db URL in app.py

#Usage 

1. Run the server

2. Use postman to interact with API 

    ENDPOINTS:
    1. /upload-csv (POST)
        parameters:
            file: (form-data) csv file
            table_name: (form-data) Target table name
    2. /batch-insert (POST)
        parameters:
            table_name: (JSON) targe table neme
            rows:(JSON) data rows
            --