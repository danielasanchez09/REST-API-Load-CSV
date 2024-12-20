import pytest
from app import app
import json 

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

#Test /upload-csv with invalid data
def test_upload_csv_invalid(client):
    response = client.post('/upload-csv', data={'file': (None, 'departments.csv'),'table_name': 'nonexistent_table'})
    assert response.status_code == 400
    assert b"error" in response.data