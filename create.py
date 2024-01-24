from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.api_core.retry import Retry

def create_dataset_if_not_exists(client, dataset_id):
    dataset_ref = client.dataset(dataset_id)
    
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' wurde gefunden.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        print(dataset)
        client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' wurde erstellt.")

def create_table_if_not_exists(client, dataset_id, table_id, schema):
    table_ref = client.dataset(dataset_id).table(table_id)
    print(table_ref)

    try:
        client.get_table(table_ref, retry=Retry(deadline=60, maximum=3))
        print(f"Tabelle '{table_id}' existiert bereits in Dataset '{dataset_id}'.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Tabelle '{table_id}' wurde in Dataset '{dataset_id}' erstellt.")