import os
from copy import deepcopy

import boto3
import pandas as pd
from fastparquet import write
import uuid

path_to_folder = os.getenv('path_to_folder')
dataset_s3_name = os.getenv('dataset_s3_name')

small_dataset = "table1k"
large_dataset = "table5m"
chunk_size = 405000
template = {
    '_id': {
        'd_oid': '100000000000000000000000'
    },
    '_version': 10,
    'acceptedDateTime': {
        'd_date': '2000-01-01T00:00:00.000Z'
    },
    'element': {
        'array': [],
        'testId': '10000000-0000-0000-0000-000000000000',
        'declaredDateTime': None,
        'type': 'aaaaaaaaAaaaaaaaaaaaa',
        'Date': {
            'date': None,
            'type': 'AAAAAAAAAAAAAAAA',
            'knownDate': None
        },
        'additionalId': None,
        'effectiveDate': {
            'date': None,
            'type': 'AAAAAAAAAAAAAAAA',
            'knownDate': None
        },
        'hasAdditionalField': None
    },
    'secondTestId': '10000000-0000-0000-0000-000000000000'
}


def create_parquet(output_data_file_name, chunk_id, json_data):
    write(
        f"{output_data_file_name}.parquet",
        pd.DataFrame({'id': chunk_id, 'val': json_data}),
    )


def upload_file_to_s3(file_location, s3_bucket, s3_key):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(file_location, s3_bucket, s3_key)


def create_hive_on_s3_data(bucket_name, s3_file_path, collection_name):
    client = boto3.client("glue", region_name='eu-west-2')

    database_name = os.getenv('db_name')
    try:
        client.delete_table(DatabaseName=database_name, Name=collection_name)
    except client.exceptions.EntityNotFoundException:
        pass

    client.create_table(
        DatabaseName=database_name,
        TableInput={
            "Name": collection_name,
            "Description": f"Hive table for analytical-env metrics data - for collection {collection_name}",
            "StorageDescriptor": {
                "Columns": [
                    {"Name": "val", "Type": "string"}
                ],
                "Location": f"s3://{bucket_name}/{s3_file_path}",
                "Compressed": False,
                "NumberOfBuckets": -1,
                "OutputFormat":"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {
                        "serialization.format": "1"
                    }
                },
            },
            "TableType": "EXTERNAL_TABLE",
        },
    )

    print(f"Successfully created Hive on table {collection_name} in database {database_name}")


def write_data_and_upload_to_s3(output_data_file_name, chunk_length, chunk_id):
    local_filename = f"{output_data_file_name}_{chunk_id}"
    json_builder = []

    for x in range(0, chunk_length):
        output_data = template
        output_data["_id"]["d_oid"] = int(uuid.uuid1())
        json_builder.append(deepcopy(output_data))
    print(json_builder)
    print(f"Successfully created json data - length {x}")

    create_parquet(local_filename, chunk_id, json_builder)
    print(f"Successfully created parquet file {local_filename}.parquet")

    upload_file_to_s3(
        f"{local_filename}.parquet",
        dataset_s3_name,
        f"{path_to_folder}/{output_data_file_name}/{local_filename}.parquet"
    )
    print(f"Successfully uploaded {local_filename}.parquet to s3")

    os.remove(f"{local_filename}.parquet")


def create_false_data(output_data_file_name, number_of_copies):
    whole_chunks = round(number_of_copies / chunk_size)
    last_chunk = number_of_copies - (whole_chunks * chunk_size)
    chunk_id = 1

    for x in range(0, whole_chunks):
        write_data_and_upload_to_s3(output_data_file_name, chunk_size, chunk_id)
        chunk_id += 1

    write_data_and_upload_to_s3(output_data_file_name, last_chunk, chunk_id)


create_false_data(small_dataset, 1000)
create_false_data(large_dataset, 5000000)
create_hive_on_s3_data(dataset_s3_name, f"{path_to_folder}/{small_dataset}/", small_dataset)
create_hive_on_s3_data(dataset_s3_name, f"{path_to_folder}/{large_dataset}/", large_dataset)
