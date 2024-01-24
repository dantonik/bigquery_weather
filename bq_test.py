import os
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
table_id = os.getenv("BIGQUERY_TABLE_ID")
# credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
credentials_path = os.path.join(os.path.dirname(__file__), 'credentials.json')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

client = bigquery.Client()

def test_bigquery_connection():
    try:
        datasets = list(client.list_datasets())
        
        if datasets:
            print("Connection to BigQuery successful. Datasets in the project:")
            for dataset in datasets:
                print(dataset.dataset_id)
        else:
            print("No datasets found in the project.")
            
    except Exception as e:
        print(f"Error connecting to BigQuery: {e}")

test_bigquery_connection()
