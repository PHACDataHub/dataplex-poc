# bring in census for health unit - median income, 
import random
from typing import List
from pathlib import Path

from utils import create_bucket, upload_blob

from mimesis import Field, Fieldset, Schema
from mimesis import Generic
from mimesis import Address
from mimesis import Datetime
from mimesis import Numeric
from mimesis import Person
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

ontario_raw_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'date_of_birth': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-100, end=current_year-1),
        'gender': generic.choice(['M', 'F', 'O']),
        # 'age': generic.choice([i for i in range(1,100,1)]),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'fever': generic.choice([True, False]),
        # 'province': address.province(),
        'province': 'Ontario',
        'vaccination_location': str(address.postal_code),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

bc_raw_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'date_of_birth': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-100, end=current_year-1),
        'gender': generic.choice(['M', 'F', 'O']),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'province': 'British Columbia',
        'vaccination_location': str(address.postal_code),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

alberta_raw_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'date_of_birth': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-100, end=current_year-1),
        'gender': generic.choice(['M', 'F', 'O']),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'province': 'Alberta',
        'vaccination_location': str(address.postal_code),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

ontario_cleaned_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'province': 'Ontario',
        # 'date_of_birth': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-100, end=current_year-1),
        'age': generic.choice([i for i in range(1,100,1)]),
        'gender': generic.choice(['M', 'F', 'O']),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'vaccination_location': str(address.postal_code),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

bc_cleaned_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'province': 'Ontario',
        # 'date_of_birth': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-100, end=current_year-1),
        'age': generic.choice([i for i in range(1,100,1)]),
        'gender': generic.choice(['M', 'F', 'O']),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'vaccination_location': str(address.postal_code),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

alberta_cleaned_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'province': 'Ontario',
        # 'date_of_birth': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-100, end=current_year-1),
        'age': generic.choice([i for i in range(1,100,1)]),
        'gender': generic.choice(['M', 'F', 'O']),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'vaccination_location': str(address.postal_code),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

ontario_final_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'province': 'Ontario',
        'gender': generic.choice(['M', 'F', 'O']),
        'age_range': generic.choice(['0-1', '2-5', '6-12', '12-17', '18-25', '26-35','36-45', '46-55', '56-65', '66-75', '76-85', '86-95', '95 +']),
        'health_region': generic.choice([True, False]),
        'health_sub_region': str(address.postal_code),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

bc_final_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'province': 'British Columbia',
        'gender': generic.choice(['M', 'F', 'O']),
        'age_range': generic.choice(['0-1', '2-5', '6-12', '12-17', '18-25', '26-35','36-45', '46-55', '56-65', '66-75', '76-85', '86-95', '95 +']),
        'health_region': generic.choice([True, False]),
        'health_sub_region': str(address.postal_code),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)


alberta_final_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'province': 'Alberta',
        'gender': generic.choice(['M', 'F', 'O']),
        'age_range': generic.choice(['0-1', '2-5', '6-12', '12-17', '18-25', '26-35','36-45', '46-55', '56-65', '66-75', '76-85', '86-95', '95 +']),
        'health_region': generic.choice([True, False]),
        'health_sub_region': str(address.postal_code),
        'vaccination_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'manufacturer_id': generic.choice([i for i in range(10000,99999,1)]),
        'batch_number': generic.choice([i for i in range(10000,99999,1)]),
    }
)

if __name__ == "__main__":

    # -- raw --
    ontario_raw_schema = ontario_raw_fields(1000, 2023)
    df = pd.DataFrame(ontario_raw_schema.create())
    df.to_parquet("./data/ontario_seasonal_influenza_vaccine_coverage_raw.csv", index=False)

    bc_raw_schema = bc_raw_fields(1000, 2023)
    df = pd.DataFrame(bc_raw_schema.create())
    df.to_parquet("./data/bc_seasonal_influenza_vaccine_coverage_raw.csv", index=False)

    alberta_raw_schema = alberta_raw_fields(1000, 2023)
    df = pd.DataFrame(alberta_raw_schema.create())
    df.to_parquet("./data/alberta_seasonal_influenza_vaccine_coverage_raw.csv", index=False)

    # -- cleaned --
    ontario_cleaned_schema = ontario_cleaned_fields(1000, 2023) 
    df = pd.DataFrame(ontario_cleaned_schema.create())
    df.to_parquet("./data/ontario_seasonal_influenza_vaccine_coverage_cleaned.parquet", engine="pyarrow")

    bc_cleaned_schema = bc_cleaned_fields(1000, 2023) 
    df = pd.DataFrame(bc_cleaned_schema.create())
    df.to_parquet("./data/bc_seasonal_influenza_vaccine_coverage_cleaned.parquet", engine="pyarrow")

    alberta_cleaned_schema = ontario_cleaned_fields(1000, 2023) 
    df = pd.DataFrame(alberta_cleaned_schema.create())
    df.to_parquet("./data/alberta_seasonal_influenza_vaccine_coverage_cleaned.parquet", engine="pyarrow")

    # -- final --
    ontario_final_schema = ontario_final_fields(1000, 2023) 
    df = pd.DataFrame(ontario_final_schema.create())
    df.to_parquet("./data/ontario_seasonal_influenza_vaccine_coverage_final.parquet", engine="pyarrow")

    bc_final_schema = bc_final_fields(1000, 2023) 
    df = pd.DataFrame(bc_cleaned_schema.create())
    df.to_parquet("./data/bc_seasonal_influenza_vaccine_coverage_final.parquet", engine="pyarrow")

    alberta_final_schema = ontario_cleaned_fields(1000, 2023) 
    df = pd.DataFrame(alberta_final_schema.create())
    df.to_parquet("./data/alberta_seasonal_influenza_vaccine_coverage_final.parquet", engine="pyarrow")

    # save to bucket
    bucket_name = 'phx-dp-seasonal_influenza_vaccine_coverage'
    create_bucket(bucket_name)

    source_file_name = "./data/ontario_seasonal_influenza_vaccine_coverage_raw.csv"
    destination_blob_name = 'ontario_seasonal_influenza_vaccine_coverage_raw.csv'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/bc_seasonal_influenza_vaccine_coverage_raw.csv"
    destination_blob_name = 'bc_seasonal_influenza_vaccine_coverage_raw.csv'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/alberta_seasonal_influenza_vaccine_coverage_raw.csv"
    destination_blob_name = 'alberta_seasonal_influenza_vaccine_coverage_raw.csv'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/ontario_seasonal_influenza_vaccine_coverage_cleaned.parquet"
    destination_blob_name = 'ontario_seasonal_influenza_vaccine_coverage_cleaned.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/bc_seasonal_influenza_vaccine_coverage_cleaned.parquet"
    destination_blob_name = 'bc_seasonal_influenza_vaccine_coverage_cleaned.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/alberta_seasonal_influenza_vaccine_coverage_cleaned.parquet"
    destination_blob_name = 'alberta_seasonal_influenza_vaccine_coverage_cleaned.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/ontario_seasonal_influenza_vaccine_coverage_final.parquet"
    destination_blob_name = 'ontario_seasonal_influenza_vaccine_coverage_final.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/bc_seasonal_influenza_vaccine_coverage_final.parquet"
    destination_blob_name = 'bc_seasonal_influenza_vaccine_coverage_final.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = "./data/alberta_seasonal_influenza_vaccine_coverage_final.parquet"
    destination_blob_name = 'alberta_seasonal_influenza_vaccine_coverage_final.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)