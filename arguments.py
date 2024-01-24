from datetime import datetime
import argparse
import re
import os

def validate_parameters(start_date, end_date, location, api_key, credentials_path, big_query_target_id):
    if not all([start_date, end_date, location, api_key, credentials_path, big_query_target_id]):
        return False, "Alle Parameter müssen angegeben werden."

    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return False, "START_DATE und END_DATE müssen das Format 'YYYY-MM-DD' haben."

    if not os.path.isfile(credentials_path):
        return False, "GOOGLE_APPLICATION_CREDENTIALS_PATH ist keine gültige Datei."

    if not re.match(r'^[A-Za-z0-9_\-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$', big_query_target_id):
        return False, "BIG_QUERY_TARGET_ID muss das Format 'PROJECT.DATASET.TABLE' haben."

    return True, "Alle Parameter sind gültig."

def parse_arguments():
    parser = argparse.ArgumentParser(description="Programm zum Überprüfen von Parametern.")
    parser.add_argument("--start_date", type=str, help="Startdatum im Format 'YYYY-MM-DD'")
    parser.add_argument("--end_date", type=str, help="Enddatum im Format 'YYYY-MM-DD'")
    parser.add_argument("--location", type=str, help="Ort")
    parser.add_argument("--api_key", type=str, help="API-Schlüssel")
    parser.add_argument("--credentials_path", type=str, help="Pfad zu den Google-Anmeldeinformationen")
    parser.add_argument("--big_query_target_id", type=str, help="BigQuery-Ziel-ID im Format 'PROJECT.DATASET.TABLE'")
    parser.add_argument("--env", action="store_true", help="Verwenden Sie die .env-Datei für Parameter")
    parser.add_argument("--use_env_file", action="store_true", help="Verwenden Sie die .env-Datei für Parameter")

    args = parser.parse_args()
    return args

def split_big_query_target_id(target_id):
    try:
        project_id, rest = target_id.split(".", 1)
        dataset_id, table_id = rest.split(".", 1)
        return project_id, dataset_id, table_id
    except ValueError:
        return None, None, None