# https://www.canada.ca/en/public-health/services/chronic-diseases/cancer/cancer-young-people-canada-program.html
# https://health-infobase.canada.ca/data-tools/cypc/#:~:text=The%20Cancer%20in%20Young%20People,planning%20for%20cancer%20control%20efforts.
import random
from typing import List
from pathlib import Path

from utils.utils import save_to_bucket
from utils.options import pt, health_unit

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

cases_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        'client_id': numeric.increment(),
        'sex': generic.choice(['M', 'F']),
        'pt': generic.choice(pt),
        'health_unit': generic.choice(health_unit),
        'diagnosis_age': generic.choice([i for i in range(1,40,1)]),
        'diagnosis_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        # 'lymphoid leukemias'
        # 'acute myeloid leukemias'
        # 'chronic myeloproliferative disease'
        # 'myelodysplastic syndrome and other myeloproliferative diseases'
        # 'unspecified and other specified leukemias'
        # 'hodgkin_lymphomas'
        # 'non-hodgkin_lymphomas'
        # 'burkitt_lymphoma'
        'leukemias_myeloproliferative_diseases_and_myelodysplastic_diseases': generic.choice([0, 1]),
        'lymphomas_and_reticuloendothelial_neoplasm': generic.choice([0, 1]),
        'CNS_and_miscellaneous_intracranial_and_intraspinal_neoplasms': generic.choice([0, 1]),
        'CNS_and_miscellaneous_intracranial_and_intraspinal_neoplasms': generic.choice([0, 1]),
        'neuroblastoma_and_other_peripheral_nervous_cell_tumors': generic.choice([0, 1]),
        'retinoblastoma': generic.choice([0, 1]),
        'renal_tumors': generic.choice([0, 1]),
        'hepatic_tumors': generic.choice([0, 1]),
        'malignant_bone_tumors': generic.choice([0, 1]),
        'soft_tissue_and_other_extraosseous_sarcomas': generic.choice([0, 1]),
        'germ_cell_tumors_trophoblastic_tumors_and_neoplasms_of_gonads': generic.choice([0, 1]),
        'other_malignant_epithelial_neoplasms_and_malignant_melanomas': generic.choice([0, 1]),
        'other_and_unspecified_malignant_neoplasms': generic.choice([0, 1]),
    }
)

mortality_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        'age': generic.choice([i for i in range(1,18,1)]),
        'sex': generic.choice(['M', 'F']),
        'pt': generic.choice(pt),
        'health_unit': generic.choice(health_unit),
        'diagnosis_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-7, end=current_year-1),
        'date_of_death': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'leukemias_myeloproliferative_diseases_and_myelodysplastic_diseases': generic.choice([0, 1]),
        'lymphomas_and_reticuloendothelial_neoplasm': generic.choice([0, 1]),
        'CNS_and_miscellaneous_intracranial_and_intraspinal_neoplasms': generic.choice([0, 1]),
        'CNS_and_miscellaneous_intracranial_and_intraspinal_neoplasms': generic.choice([0, 1]),
        'neuroblastoma_and_other_peripheral_nervous_cell_tumors': generic.choice([0, 1]),
        'retinoblastoma': generic.choice([0, 1]),
        'renal_tumors': generic.choice([0, 1]),
        'hepatic_tumors': generic.choice([0, 1]),
        'malignant_bone_tumors': generic.choice([0, 1]),
        'soft_tissue_and_other_extraosseous_sarcomas': generic.choice([0, 1]),
        'germ_cell_tumors_trophoblastic_tumors_and_neoplasms_of_gonads': generic.choice([0, 1]),
        'other_malignant_epithelial_neoplasms_and_malignant_melanomas': generic.choice([0, 1]),
        'other_and_unspecified_malignant_neoplasms': generic.choice([0, 1]),
    }
)


if __name__ == "__main__":

    # Save to file
    cases_fields_schema = cases_fields(1000, 2023)
    df = pd.DataFrame(cases_fields_schema.create())
    df.to_csv("./data/cancer_in_young_people_cases.csv", index=False)
    df.to_parquet("./data/cancer_in_young_people_cases.parquet", engine="pyarrow")

    mortality_schema = mortality_fields(1000, 2023)
    df = pd.DataFrame(mortality_schema.create())
    df.to_csv("./data/cancer_in_young_people_mortality.csv", index=False)
    df.to_parquet("./data/cancer_in_young_people_mortality.parquet", engine="pyarrow")

    # Save to bucket
    save_to_bucket('cancer_in_young_people_cases.csv','dataplexpoc-cancer-in-young-people-cases-raw')
    save_to_bucket('cancer_in_young_people_mortality.csv','dataplexpoc-cancer-in-young-people-mortality-raw')
    save_to_bucket('cancer_in_young_people_cases.parquet','dataplexpoc-cancer-in-young-people-cases-final')
    save_to_bucket('cancer_in_young_people_mortality.parquet','dataplexpoc-cancer-in-young-people-mortality-final')


# #  UPLOAD ASSET (in terminal)
# export LAKE_NAME=health-promotion-and-chronic-disease-prevention

# export ZONE_NAME=cancer-in-young-people-raw
# export ASSET_NAME=cancer-in-young-people-cases-raw-table
# export BUCKET_NAME=dataplexpoc-cancer-in-young-people-cases-raw

# export ZONE_NAME=cancer-in-young-people-raw
# export ASSET_NAME=cancer-in-young-people-mortality-raw-table
# export BUCKET_NAME=dataplexpoc-cancer-in-young-people-mortality-raw

# export ZONE_NAME=cancer-in-young-people-curated
# export ASSET_NAME=cancer-in-young-people-cases-final-table
# export BUCKET_NAME=dataplexpoc-cancer-in-young-people-cases-final

# export ZONE_NAME=cancer-in-young-people-curated
# export ASSET_NAME=cancer-in-young-people-mortality-final-table
# export BUCKET_NAME=dataplexpoc-cancer-in-young-people-mortality-final

# gcloud dataplex assets create $ASSET_NAME \
# --project=$PROJECT_ID \
# --location=$LOCATION \
# --lake=$LAKE_NAME \
# --zone=$ZONE_NAME \
# --resource-type=STORAGE_BUCKET \
# --resource-name=projects/$PROJECT_ID/buckets/$BUCKET_NAME \
# --discovery-enabled 