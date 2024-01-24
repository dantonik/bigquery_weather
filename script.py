import os
from dotenv import load_dotenv
from google.cloud import bigquery
import json
from datetime import datetime
import pytz
import re

from arguments import parse_arguments, validate_parameters, split_big_query_target_id
from weather_api import fetch_weather_data
from create import create_dataset_if_not_exists, create_table_if_not_exists

bigquery.SchemaField("start_date", "DATE"),
bigquery.SchemaField("query_cost", "FLOAT"),
bigquery.SchemaField("average_temp", "FLOAT"),
bigquery.SchemaField("max_cloudcover", "INTEGER"),
bigquery.SchemaField("fog", "BOOLEAN"),
bigquery.SchemaField("days_with_drizzle", "INTEGER"),
bigquery.SchemaField("runtime_timestamp", "TIMESTAMP"),

def write_to_bigquery():
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

    with open("batch_2", "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job = job.result()
    print(job.state)
    with open("batch_2", "r") as file:
        print("Inserted: " + str(json.load(file)))

def calculate(input):
    output = dict()
    output['resolved_address'] = input["resolvedAddress"]
    output['start_date'] = start_date
    output['query_cost'] = input["queryCost"]
    output['average_temp'] = 0
    output['max_cloudcover'] = 0
    output['fog'] = False
    output['days_with_drizzle'] = 0
    current_time = datetime.now(pytz.timezone(input['timezone']))
    current_time_utc = current_time.astimezone(pytz.UTC)
    formatted_time = current_time_utc.strftime("%Y-%m-%d %H:%M:%S.%f %Z")
    output['runtime_timestamp'] = formatted_time
    
    for i, day in enumerate(input['days']):
        output['average_temp'] += day['temp']
    output['average_temp'] /= (i + 1)
    
    for i, day in enumerate(input['days']):
        if day['cloudcover'] > output['max_cloudcover']:
            output['max_cloudcover'] = day['cloudcover']
    
    output['max_cloudcover'] = round(output['max_cloudcover'])
    
    for day in input['days']:
        with open('day_output', 'w') as file:
            json.dump(day, file)
        for hour in day['hours']:
            if hour['icon'] == 'Fog':
                output['fog'] = True
                break
        if output['fog'] == True:
            break

    for day in input['days']:
        with open('day_output', 'w') as file:
            json.dump(day, file)
        for hour in day['hours']:
            if re.search(r'Drizzle', hour['conditions']):
                output['days_with_drizzle'] += 1
                break

    test = [1]
    test[0] = output
    with open('batch_1', 'w') as file:
        json.dump(test, file)
    with open('batch_2', 'w') as file:
        json.dump(output, file)
    write_to_bigquery()

if __name__ == "__main__":
    args = parse_arguments()

    if args.use_env_file:
        load_dotenv(override=True)

    start_date = args.start_date or os.getenv("START_DATE")
    end_date = args.end_date or os.getenv("END_DATE")
    location = args.location or os.getenv("LOCATION")
    api_key = args.api_key or os.getenv("API_KEY")
    credentials_path = args.credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_PATH")
    big_query_target_id = args.big_query_target_id or os.getenv("BIG_QUERY_TARGET_ID")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    project_id, dataset_id, table_id = split_big_query_target_id(big_query_target_id)
    is_valid, message = validate_parameters(start_date, end_date, location, api_key, credentials_path, big_query_target_id)
    print(message)

    with open(credentials_path) as json_file:
        credentials_info = json.load(json_file)

    client = bigquery.Client.from_service_account_info(
        credentials_info,
        project=project_id
    )

    create_dataset_if_not_exists(client, dataset_id)

    table_schema = [
        bigquery.SchemaField("resolved_address", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("query_cost", "FLOAT"),
        bigquery.SchemaField("average_temp", "FLOAT"),
        bigquery.SchemaField("max_cloudcover", "INTEGER"),
        bigquery.SchemaField("fog", "BOOLEAN"),
        bigquery.SchemaField("days_with_drizzle", "INTEGER"),
        bigquery.SchemaField("runtime_timestamp", "TIMESTAMP"),
    ]

    file_path = 'output.json'

    create_table_if_not_exists(client, dataset_id, table_id, table_schema)

    weather_data = fetch_weather_data(start_date, end_date, location, api_key)

    with open(file_path, 'w') as file:
        json.dump(weather_data, file)

    print(f'Dictionary has been written to {file_path}')

    calculate(weather_data)