# https://health-infobase.canada.ca/health-inequalities/data-tool/

import random
from typing import List
from pathlib import Path

from utils.utils import save_to_bucket
from utils.options import pt, remoteness_index

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

health_inequalities_raw_fields = lambda num_rows, current_year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'pt': generic.choice(pt),
        'sex': generic.choice(['M', 'F']),
        'gender': generic.choice(['M', 'F', 'O']),
        'age': generic.choice([i for i in range(1,100,1)]),
        'year': dt.formatted_date(fmt="%Y", start=current_year-5, end=current_year-1),
        'brush_teeth_twice_day': generic.choice([1, 0]),
        'alcohol_use_heavy_drinking': generic.choice([1, 0]),
        'alcohol_use_occasional_drinking': generic.choice([1, 0]),
        'fruit_veg_consumption_5_12_a_day': generic.choice([1, 0]),
        'self_reported_physically_active': generic.choice([0, 2, 3, 4, 5]),
        'smoking_daily_or_occasionaly': generic.choice([1, 0]),
        'remoteness_index': generic.choice(remoteness_index),
        # 'indiginious_identity': 
        'living_arrangments': generic.choice(['living with 2 parents', 'in single parent family', 'living alone', 'living with others', 'other']),
        'first_official_language_spoken': generic.choice(['english', 'french', 'neither']),
        'dwelling_owned_by_a_household_memeber': generic.choice([1, 0]),
        'household_education': generic.choice(['less than high school', 'high school graduate', 'comunity college, technical school', 'university gaduate','missing']),
        'diabetes': generic.choice([1, 0]),
        'arthritis': generic.choice([1, 0]),
        'heart disease': generic.choice([1, 0]),
        'hiv': generic.choice([1, 0]),
        'lung cancer': generic.choice([1, 0]),
    }
)


if __name__ == "__main__":

    # Save to file
    health_inequalities_raw_schema = health_inequalities_raw_fields(1000, 2023)
    df = pd.DataFrame(health_inequalities_raw_schema.create())
    df.to_csv("./data/health_inequalities_raw_schema.csv", index=False)
    df.to_parquet("./data/health_inequalities_curated_schema.parquet", engine="pyarrow")

    # Save to bucket
    save_to_bucket('health_inequalities_raw_schema.csv','dataplexpoc-health_inequalities_raw')
    save_to_bucket('health_inequalities_curated_schema.parquet','dataplexpoc-lhealth_inequalities_curated')
