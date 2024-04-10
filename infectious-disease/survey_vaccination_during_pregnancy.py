import random
from typing import List
from pathlib import Path

from utils import create_bucket, upload_blob
from options import health_unit, pt, self_described_health, vaccine_status

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

vaccine_survey_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'survey_completion_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'province_territory': generic.choice(pt),
        'number_of_pregnancies': generic.choice([i for i in range(1,8,1)]),
        'household_income': generic.choice([i for i in range(5000,200000,1)]),
        'median_household_income_of_repondants_geo': generic.choice([i for i in range(20000,90000,1)]),
        'vaccination_status_routine': generic.choice(vaccine_status),
        'vaccination_status_annual_influenza': generic.choice(vaccine_status),
        'vaccination_status_covid19': generic.choice(vaccine_status),
        'self_described_health': generic.choice(self_described_health),
        'vaccine_during_pregnancy': generic.choice(['covid-19', 'monkey pox', 'yellow fever', 'MMR', 'Hepatitis A', 'Hepatitis A']),
        'weeks_gestation': generic.choice([i for i in range(1,42,1)]),
        'side_effects': generic.choice(['none', 'other', 'exhaustion', 'rash', 'nausea', 'vommiting']),
        'reason_for_vaccination': text.sentence(),
    }
)

if __name__ == "__main__":
    vaccine_survey_schema = vaccine_survey_fields(1000, 2023)
    df = pd.DataFrame(vaccine_survey_schema.create())
    df.to_parquet("./data/survey_of_vaccination_during_pregancy.parquet", engine="pyarrow")

    bucket_name = 'phx-dp-vaccination-during-pregancy-survey'
    create_bucket(bucket_name)

    source_file_name = './data/survey_of_vaccination_during_pregancy.parquet'
    destination_blob_name = 'survey_of_vaccination_during_pregancy.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

