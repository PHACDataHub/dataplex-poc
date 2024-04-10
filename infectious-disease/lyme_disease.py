import random
from typing import List
from pathlib import Path

from utils import create_bucket, upload_blob

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

lyme_disease_reporting_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'diagnosis_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'fever': generic.choice([True, False]),
        'location_of_tick_bite': address.postal_code(),
        'age_at_diagnosis': generic.choice([i for i in range(1,100,1)]),
        'treated_when_bit': generic.choice(['yes', 'no', 'not applicable']),
        'symptoms': generic.choice(['fever', 'headache', 'fatigue', 'rash']),
    }
)

lab_extract_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        'client_id': numeric.increment(),
        'Episode_Accurate_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Diagnosis_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Specimen_Type': generic.choice(['blood sample', 'tick']),
        'Test_Name': generic.choice(["IMMUNOLOGY/SEROLOGY", "MICROBIOLOGY", "MOLECULAR METHODS"]),
        'Collection_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        # 'Resulted_Test_Codes_Description': generic.choice(res_test_code_desc),
        'Lab_Test_Results_Description': generic.choice(['confirmed', 'inconclusive',]),
    },iterations=num_rows)

lyme_disease_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        'client_id': numeric.increment(),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'diagnosis_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'fever': generic.choice([True, False]),
        'location_of_tick_bite': address.postal_code(),
        'age_at_diagnosis': generic.choice([i for i in range(1,100,1)]),
        'treated_when_bit': generic.choice(['yes', 'no', 'not applicable']),
        'symptoms': generic.choice(['fever', 'headache', 'fatigue', 'rash']),
        'Episode_Accurate_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Specimen_Type': generic.choice(['blood sample', 'tick']),
        'Test_Name': generic.choice(["IMMUNOLOGY/SEROLOGY", "MICROBIOLOGY", "MOLECULAR METHODS"]),
        'Collection_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        # 'Resulted_Test_Codes_Description': generic.choice(res_test_code_desc),
        'Lab_Test_Results_Description': generic.choice(['confirmed', 'inconclusive', 'NA']),
    },iterations=num_rows)

if __name__ == "__main__":
    lyme_disease_reported_schema = lyme_disease_reporting_fields(1000, 2023)
    df = pd.DataFrame(lyme_disease_reported_schema.create())
    df.to_parquet("./data/lyme_disease_reported.parquet", engine="pyarrow")

    lyme_disease_lab_extract_schema = lab_extract_fields(1000, 2023)
    df = pd.DataFrame(lyme_disease_lab_extract_schema.create())
    df.to_parquet("./data/lyme_disease_lab_extract.parquet", engine="pyarrow")

    lyme_disease_schema = lyme_disease_fields(1000, 2023)
    df = pd.DataFrame(lyme_disease_schema.create())
    df.to_parquet("./data/lyme_disease.parquet", engine="pyarrow")

    bucket_name = 'phx-dp-lyme-disease'
    create_bucket(bucket_name)

    source_file_name = './data/lyme_disease_reported.parquet'
    destination_blob_name = 'working/lyme_disease_reported.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = './data/lyme_disease_lab_extract.parquet'
    destination_blob_name = 'working/lyme_disease_lab_extract.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = './data/lyme_disease.parquet'
    destination_blob_name = 'final/lyme_disease.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)


