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

field = Field(locale=Locale.EN_CA)
fieldset = Fieldset(locale=Locale.EN_CA)
generic = Generic(locale=Locale.EN_CA)
address = Address(locale=Locale.EN_CA)
dt = Datetime(locale=Locale.EN_CA)
numeric = Numeric()
text = Text()

lyme_disease_fields = lambda num_rows, year: Schema(
    schema = lambda : {
        'client_id': numeric.increment(),
        'gender': ['M', 'F', 'O'],
        'age': generic.choice([i for i in range(1,100,1)]),
        'diagnosis_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'fever': generic.choice([True, False]),
        'location_of_tick_bite': address.province(),
        'age_at_diagnosis': generic.choice([i for i in range(1,100,1)]),
        'treated_when_bit': generic.choice(['yes', 'no', 'not applicable']),
        'symptoms': generic.choice(['fever', 'headache', 'fatigue', 'rash']),
    }
)

if __name__ == "__main__":
    lyme_disease_schema = lyme_disease_fields(1000, 2023)
    df = pd.DataFrame()