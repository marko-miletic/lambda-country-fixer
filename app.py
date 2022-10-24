from chalice import Chalice
import boto3
import pandas as pd
from chalicelib import dataframes_correction
from chalicelib import standard_values
from io import BytesIO
from io import StringIO
import os


app = Chalice(app_name='bla-ha-ha')

ACCESS_KEY='test'
SECRET_KEY='test'
SESSION_TOKEN='test'

INPUT_BUCKET='input'
OUTPUT_BUCKET='output'

s3 = boto3.resource('s3',
                    endpoint_url="http://host.docker.internal:4566",
                    use_ssl=False,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    aws_session_token=SESSION_TOKEN)

s3_client = boto3.client('s3',
                    endpoint_url="http://host.docker.internal:4566",
                    use_ssl=False,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    aws_session_token=SESSION_TOKEN)


@app.on_s3_event(bucket=INPUT_BUCKET)
def handler(event):
    pd.options.mode.chained_assignment = None  # default='warn'

    countries_set = standard_values.get_countries()
    codes_set_alpha3 = standard_values.get_codes_alpha3()
    codes_set_alpha2 = standard_values.get_codes_alpha2()
    country_code_combinations = standard_values.get_country_code_combinations()

    file_extension = os.path.splitext(event.key)[1]
    file_name = os.path.splitext(event.key)[0]

    func_read_extension = getattr(pd, f"read_{file_extension[1:]}")
    
    obj = s3.Object(INPUT_BUCKET, event.key)
    with BytesIO(obj.get()['Body'].read()) as bio:
        df = func_read_extension(bio)
    
    dataframes_correction.correction([df], countries_set, codes_set_alpha3, codes_set_alpha2, country_code_combinations)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3.Object(OUTPUT_BUCKET, f"{file_name}.csv").put(Body=csv_buffer.getvalue())
