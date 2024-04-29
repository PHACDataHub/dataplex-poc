# https://www.publichealthontario.ca/en/Data-and-Analysis/Infectious-Disease/COVID-19-Data-Surveillance/Wastewater

import random
from typing import List
from pathlib import Path

from utils.utils import save_to_bucket

from mimesis import Field, Fieldset, Schema
from mimesis import Generic
from mimesis import Address
from mimesis import Datetime
from mimesis import Numeric
from mimesis.locales import Locale
from mimesis import Text
from mimesis.providers.base import BaseProvider

import pandas as pd
import numpy as np

field = Field(locale=Locale.EN_CA)
fieldset = Fieldset(locale=Locale.EN_CA)
generic = Generic(locale=Locale.EN_CA)
address = Address(locale=Locale.EN_CA)
dt = Datetime(locale=Locale.EN_CA)
numeric = Numeric()
text = Text()

outfall_locations = [
    "Assiniboia",
    "Battleford",
    "Birch Hills",
    "Canora",
    "Estevan",
    "AZle-A-la-Crosse",
    "La Ronge",
    "Lumsden",
    "Maple Creek",
    "Meadow Lake",
    "Melville",
    "Moose Jaw",
    "North Battleford",
    "Pasqua",
    "Prince Albert",
    "Saskatoon",
    "Southey",
    "Swift Current",
    "Unity",
    "Watrous",
    "Weyburn",
    "Yorkton",
    "Bathurst",
    "Campbellton",
    "Edmundston",
    "Fredericton",
    "Miramichi",
    "Moncton",
    "Saint John",
    "Winnipeg South End",
    "Winnipeg West End",
    "Yarmouth",
    "Vancouver Iona Island",
    "Vancouver Lions Gate",
    "Vancouver Lulu Island",
    "Vancouver Northwest Langley",
    "Winnipeg North End",
    "Souris",
    "St. John's",
    "Summerside",
    "Toronto Ashbridges Bay",
    "Toronto Highland Creek",
    "Toronto Humber",
    "Toronto North Toronto",
    "Trenton",
    "Vancouver Annacis Island",
    "Montague",
    "Montreal North",
    "Montreal South",
    "Regina",
    "Edmonton Goldbar",
    "Haines Junction",
    "Halifax Dartmouth",
    "Halifax Halifax",
    "Halifax Millcove",
    "Battery Point",
    "Brandon",
    "Bridgewater",
    "Central Colchester",
    "City of Charlottetown & Town of Stratford",
    "Dominion-Bridgeport",
    "Alberton"
]

watewater_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        # 'outfall_id': numeric.increment(),
        'site_id': numeric.increment(),
        'site_name': generic.choice(outfall_locations),
        'sample_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-4, end=current_year),
        # 'concentration_of_SARS_CoV_2_gene_copies': generic.choice([str(i for i in range(0,10,0.2)), 'NA']),
        # 'concentration_of_influenza_A_gene_copies': generic.choice([str(i for i in range(0,5,0.0025)), 'NA']),
        # 'concentration_of_RSV_gene_copies': generic.choice([str(i for i in range(0,5,0.0025)), 'NA']),
        'concentration_of_SARS_CoV_2_gene_copies': generic.choice([str(np.arange(0,10,0.2)), 'NA']),
        'concentration_of_influenza_A_gene_copies': generic.choice([str(np.arange(0,2,0.02)), 'NA']),
        'concentration_of_RSV_gene_copies': generic.choice([str(np.arange(0,3,0.02)), 'NA']),
    }
)


infobase_extract_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        # 'outfall_id': numeric.increment(),
        'site_id': numeric.increment(),
        'site_name': generic.choice(outfall_locations),
        'sample_date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-4, end=current_year),
        'concentration_of_SARS_CoV_2_gene_copies': generic.choice([str(np.arange(0,10,0.2)), 'NA']),
        'level': generic.choice (['new site', 'low', 'moderate', 'high']),
        'trend': generic.choice(['increasing', 'decreasing', 'steady'])
    }
)


# wastewater_for_analysis

if __name__ == "__main__":

    # Save to file
    wastewater_schema = watewater_fields(1000, 2023)
    df = pd.DataFrame(wastewater_schema.create())
    df.to_csv("./data/wastewater.csv", index=False)
    df.to_parquet("./data/wastewater.parquet", engine="pyarrow")

    infobase_extract_schema = infobase_extract_fields(1000, 2023)
    df = pd.DataFrame(infobase_extract_schema.create())
    # df.to_csv("./data/wastewater_infobase_extract.csv", index=False)
    df.to_parquet("./data/wastewater_infobase_extract.parquet", engine="pyarrow")

    # Save to bucket
    save_to_bucket('wastewater.csv','dataplexpoc-wastewater-raw')
    save_to_bucket('wastewater.parquet','dataplexpoc-wastewater-curated')
    save_to_bucket('wastewater_infobase_extract.parquet','dataplexpoc-infobase-extract-curated')

     
# #  UPLOAD ASSET (in terminal)
# export LAKE_NAME=national-microbiology-lab

# export ZONE_NAME=covid-19-wastewater-raw
# export ASSET_NAME=covid-19-wastewater-raw-table
# export BUCKET_NAME=dataplexpoc-wastewater-raw

# export ZONE_NAME=covid-19-wastewater-curated
# export ASSET_NAME=covid-19-wastewater-cleaned-table
# export BUCKET_NAME=dataplexpoc-wastewater-curated

# export ZONE_NAME=covid-19-wastewater-curated
# export ASSET_NAME=covid-19-wastewater-infobase-extract-table
# export BUCKET_NAME=dataplexpoc-infobase-extract-curated

# gcloud dataplex assets create $ASSET_NAME \
# --project=$PROJECT_ID \
# --location=$LOCATION \
# --lake=$LAKE_NAME \
# --zone=$ZONE_NAME \
# --resource-type=STORAGE_BUCKET \
# --resource-name=projects/$PROJECT_ID/buckets/$BUCKET_NAME \
# --discovery-enabled 
 