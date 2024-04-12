import random
from typing import List
from pathlib import Path

from utils.options import health_unit, pt
from utils.utils import create_bucket, upload_blob

from mimesis import Field, Fieldset, Schema
from mimesis import Generic
from mimesis import Address
from mimesis import Datetime
from mimesis import Numeric
from mimesis.locales import Locale
from mimesis import Text
from mimesis.providers.base import BaseProvider

import pandas as pd

field = Field(locale=Locale.EN_CA)
fieldset = Fieldset(locale=Locale.EN_CA)
generic = Generic(locale=Locale.EN_CA)
address = Address(locale=Locale.EN_CA)
dt = Datetime(locale=Locale.EN_CA)
numeric = Numeric()
text = Text()
  

flu_watch_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'diagnosis_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'vacination_date': generic.choice([dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1), None]),
        'health_unit': generic.choice(health_unit),
        'strain': generic.choice(['A', 'B', 'not applicable']),
        'symptoms': generic.choice(['fever', 'headache', 'muscle pain', 'runny nose', 'sore throat', 'extreme tiredness','cough', 'other']), 
        'hospitalization': generic.choice(['T', 'F'])
    }
)

flu_watchers_reporting_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'reporting_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'province': generic.choice(pt),
        'health_unit': generic.choice(health_unit),
        'symptoms': generic.choice(['fever', 'headache', 'muscle pain', 'runny nose', 'sore throat', 'extreme tiredness','cough', 'other']), 
    }
)

if __name__ == "__main__":
    flu_watch_schema = flu_watch_fields(1000, 2023)
    flu_watch_df = pd.DataFrame(flu_watch_schema.create())
    flu_watch_df.to_parquet("./data/flu_watch.parquet", engine="pyarrow")

    flu_watchers_schema = flu_watchers_reporting_fields(1000, 2023)
    flu_watchers_df = pd.DataFrame(flu_watchers_schema.create())
    flu_watchers_df.to_parquet("./data/flu_watchers_reporting.parquet", engine="pyarrow")

    bucket_name = 'phx-dp-flu-watch'
    create_bucket(bucket_name)

    source_file_name = './data/flu_watchers_reporting.parquet'
    destination_blob_name = 'flu_watchers_reporting.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = './data/flu_watch.parquet'
    destination_blob_name = 'flu_watch.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)


    
