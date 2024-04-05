import random
from typing import List
from pathlib import Path

from mimesis import Field, Fieldset, Schema
from mimesis import Generic
from mimesis import Address
from mimesis import Datetime
from mimesis import Numeric
from mimesis.locales import Locale
from mimesis import Text
from mimesis.providers.base import BaseProvider

import pandas as pd

from options import sex, health_unit, pt, self_described_health, vaccine_status

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
        'sex': generic.choice(sex),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'province_territory': generic.choice(pt),
        'marriage_status': generic.choice(['M', 'D', 'W', 'S']),
        'number_of_household_members': generic.choice([i for i in range(1,8,1)]),
        'household_members_0-6': generic.choice([i for i in range(1,4,1)]),
        'household_members_7-18': generic.choice([i for i in range(1,3,1)]),
        'survey_completion_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'household_income': generic.choice([i for i in range(5000,200000,1)]),
        'median_household_income_of_repondants_geo': generic.choice([i for i in range(20000,90000,1)]),
        'vaccination_status_routine': generic.choice(vaccine_status),
        'vaccination_status_annual_influenza': generic.choice(vaccine_status),
        'vaccination_status_covid19': generic.choice(vaccine_status),
        'self_described_health': generic.choice(self_described_health),
        'vaccine_routine_sentiment': text.sentence(),
    }
)

if __name__ == "__main__":
    vaccine_survey_schema = vaccine_survey_fields(1000, 2023)
    df = pd.DataFrame(vaccine_survey_schema.create())
    df.to_parquet("../vaccine_survey.parquet", engine="pyarrow")

