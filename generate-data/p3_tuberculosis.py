import random
from typing import List
from pathlib import Path

from utils.utils import save_to_bucket, attach_asset_to_zone
from utils.options import health_unit, pt

from mimesis import Field, Fieldset, Schema
from mimesis import Generic
from mimesis import Address
from mimesis import Datetime
from mimesis import Numeric
from mimesis.locales import Locale
from mimesis import Text
from mimesis.providers.base import BaseProvider

import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_NAME = os.getenv('PROJECT3_NAME')
PROJECT_ID = os.getenv('PROJECT3_ID')
SERVICE_ACCOUNT_KEY_PATH = os.getenv('PROJECT3_SERVICE_ACCOUNT_KEY_PATH')

field = Field(locale=Locale.EN_CA)
fieldset = Fieldset(locale=Locale.EN_CA)
generic = Generic(locale=Locale.EN_CA)
address = Address(locale=Locale.EN_CA)
dt = Datetime(locale=Locale.EN_CA)
numeric = Numeric()
text = Text()

# We don't use this one
CLIENT_ID = lambda x: [random.randint(1_000_000, 9_999_999) for i in range(0,x)]

# PHAC case ID
EPISODE_ID = lambda x: [random.randint(1_000_000, 9_999_999) for i in range(0,x)]

spec_type = [
    "Aspirate",
    "Biopsy",
    "Bone Marrow",
    "Bronchial Alveolar Lavage",
    "Bronchial Washing",
    "Cerebrospinal Fluid (CSF)",
    "Discharge",
    "Endotracheal Aspirate",
    "Faeces",
    "Gastric Washing",
    "Induced Sputum",
    "Lavage",
    "Other Specimen",
    "Other Sterile Body Fluid",
    "Peritoneal fluid",
    "Pleural Fluid",
    "Pustular Fluid",
    "Sputum",
    "Stool",
    "Swab",
    "Tissue",
    "Tissue Biopsy",
    "Urine", 
]    

res_test_code_desc = [
    " ",
    "Ampli Mycobacterium TB Direct",
    "Culture - Bacterial",
    "Culture - Mycotic",
    "Microscopy - AFB",
    "MIRU - Myco Inter Rep Units",
    "MTB Complex  MTBDRplus",
    "PCR - Polymerase Chain React",
] 

lab_test_res_desc = [
    "0",
    "1+",
    "2+",
    "3+",
    "4+",
    "DETECTED",
    "FEW",
    "INDETERMINATE",
    "ISOLATED",
    "MATCH",
    "MODERATE",
    "NEGATIVE",
    "NO GROWTH",
    "NOT DETECTED",
    "NOT ISOLATED",
    "NUMEROUS",
    "POSITIVE",
    "rRNA OF MTBC DETECTED",
    "UNIQUE",
]    

lab_extract_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        # 'Client_ID': generic.choice(client_ids),
        # 'Episode_ID': generic.choice(client_ids),
        'Episode_Accurate_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Diagnosis_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Specimen_Type': generic.choice(spec_type),
        # 'Body_Site': generic.choice(TB_BODY_SITE_CATEGORY_PHAC),
        'Test_Name': generic.choice(["IMMUNOLOGY/SEROLOGY", "MICROBIOLOGY", "MOLECULAR METHODS"]),
        'Collection_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Resulted_Test_Codes_Description': generic.choice(res_test_code_desc),
        'Lab_Test_Results_Description': generic.choice(lab_test_res_desc),
    },iterations=num_rows)

episode_info_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        # 'Client ID': generic.choice(client_ids),
        # 'Client ID': CLIENT_ID,
        # 'Episode ID': generic.choice(episode_ids),
        # 'Episode ID': EPISODE_ID,       
        'Client_DOB_Month_Year': dt.formatted_date(fmt="%m-%Y", start=current_year-100, end=current_year-1),
        'Client_Gender': generic.choice(["MALE", "FEMALE"]),
        'Client_Death_Date_Month_Year': dt.formatted_date(fmt="%m-%Y", start=current_year-5, end=current_year-1),
        # 'Episode Death Cause Description': generic.choice(ep_death_cause_desc), 
        'Forward_Sortation_Area':  address.postal_code(),
        'Client_Age_at_Time_of_Diagnosis': numeric.float_number(start=0.0, end=105.0, precision=1),
        'Episode_Accurate_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_Start_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_Date_Type': generic.choice(["ONSET", "SPECIMEN", "REPORTED"]),
        'TB_Episode_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_External_Reference_Number': dt.formatted_date(fmt="%d-%m-%Y"),
        'Episode_Type_Description': generic.choice(["CASE", "CONTACT"]),
        # 'Episode Status Description': generic.choice(ep_status_desc),
        'Diagnosis_Status_Description': generic.choice(["CONFIRMED"]),
        'Diagnosis_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        # 'Episode Detection Method Description': generic.choice(ep_detect_method_desc),
        'Diagnosing_Health_Unit_Area_Description': generic.choice(health_unit),
        'Responsible_Health_Unit_Area_Description': generic.choice(health_unit),
        'Client_Birth_Province': address.province(),
        # 'Client Origin': generic.choice(origin),
        "First_Nations_Status": generic.choice(["ON RESERVE MOST OF TIME - NO", "ON RESERVE MOST OF TIME - YES"]),
        'Client_Immigration_Country_From': address.country(),
        'Client_Immigration_Country_Last_Reside': address.country(),
        'Client_Immigration_Birth_Country': address.country(),
        'Client_Immigration_Reported_Surveillance_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_Updates_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-2, end=current_year-1), 
    }
)

merged_fields = lambda num_rows, current_year: Schema(
    schema = lambda: {
        'Episode_Accurate_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Diagnosis_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Specimen_Type': generic.choice(spec_type),
        # 'Body_Site': generic.choice(TB_BODY_SITE_CATEGORY_PHAC),
        'Test_Name': generic.choice(["IMMUNOLOGY/SEROLOGY", "MICROBIOLOGY", "MOLECULAR METHODS"]),
        'Collection_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-3, end=current_year-1),
        'Resulted_Test_Codes_Description': generic.choice(res_test_code_desc),
        'Lab_Test_Results_Description': generic.choice(lab_test_res_desc),

        'Client_DOB_Month_Year': dt.formatted_date(fmt="%m-%Y", start=current_year-100, end=current_year-1),
        'Client_Gender': generic.choice(["MALE", "FEMALE"]),
        'Client_Death_Date_Month_Year': dt.formatted_date(fmt="%m-%Y", start=current_year-5, end=current_year-1),
        # 'Episode Death Cause Description': generic.choice(ep_death_cause_desc), 
        'Forward_Sortation_Area':  address.postal_code(),
        'Client_Age_at_Time_of_Diagnosis': numeric.float_number(start=0.0, end=105.0, precision=1),
        'Episode_Accurate_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_Start_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_Date_Type': generic.choice(["ONSET", "SPECIMEN", "REPORTED"]),
        'TB_Episode_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_External_Reference_Number': dt.formatted_date(fmt="%d-%m-%Y"),
        'Episode_Type_Description': generic.choice(["CASE", "CONTACT"]),
        # 'Episode Status Description': generic.choice(ep_status_desc),
        'Diagnosis_Status_Description': generic.choice(["CONFIRMED"]),
        'Diagnosis_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        # 'Episode Detection Method Description': generic.choice(ep_detect_method_desc),
        'Diagnosing_Health_Unit_Area_Description': generic.choice(health_unit),
        'Responsible_Health_Unit_Area_Description': generic.choice(health_unit),
        'Client_Birth_Province': address.province(),
        # 'Client Origin': generic.choice(origin),
        "First_Nations_Status": generic.choice(["ON RESERVE MOST OF TIME - NO", "ON RESERVE MOST OF TIME - YES"]),
        'Client_Immigration_Country_From': address.country(),
        'Client_Immigration_Country_Last_Reside': address.country(),
        'Client_Immigration_Birth_Country': address.country(),

        'Client_Immigration_Reported_Surveillance_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode_Updates_Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-2, end=current_year-1), 
    }
)

if __name__ == "__main__":

    # Save to file
    tuberculosis_outcomes_schema = episode_info_fields(1000, 2023)
    df = pd.DataFrame(tuberculosis_outcomes_schema.create())
    df.to_csv("./data/tuberculosis_outcomes.csv", index=False)
    df.to_parquet("./data/tuberculosis_outcomes.parquet", engine="pyarrow")

    tuberculosis_cases_schema = lab_extract_fields(1000, 2023)
    df = pd.DataFrame(tuberculosis_cases_schema.create())
    df.to_csv("./data/tuberculosis_cases.csv", index=False)
    df.to_parquet("./data/tuberculosis_cases.parquet", engine="pyarrow")

    tuberculosis_merged_schema = merged_fields(1000, 2023)
    df = pd.DataFrame(tuberculosis_merged_schema.create())
    df.to_parquet("./data/tuberculosis_merged_FINAL.parquet", engine="pyarrow")

    # save to bucket and load asset

    zone_name= f'{PROJECT_NAME}-raw'
    bucket_name = "tuberculosis-raw"

    save_to_bucket('tuberculosis_cases.csv', bucket_name, SERVICE_ACCOUNT_KEY_PATH)
    attach_asset_to_zone(PROJECT_ID, PROJECT_NAME, zone_name, asset_name=bucket_name, bucket_name=bucket_name, service_account_key_path=SERVICE_ACCOUNT_KEY_PATH)

    save_to_bucket('tuberculosis_outcomes.csv', bucket_name, SERVICE_ACCOUNT_KEY_PATH)
    attach_asset_to_zone(PROJECT_ID, PROJECT_NAME, zone_name, asset_name=bucket_name, bucket_name=bucket_name, service_account_key_path=SERVICE_ACCOUNT_KEY_PATH)


    zone_name= f'{PROJECT_NAME}-curated'
    
    bucket_name = "tuberculosis-final"

    save_to_bucket('tuberculosis_cases.parquet', bucket_name, SERVICE_ACCOUNT_KEY_PATH)
    attach_asset_to_zone(PROJECT_ID, PROJECT_NAME, zone_name, asset_name=bucket_name, bucket_name=bucket_name, service_account_key_path=SERVICE_ACCOUNT_KEY_PATH)

    save_to_bucket('tuberculosis_outcomes.parquet', bucket_name, SERVICE_ACCOUNT_KEY_PATH)
    attach_asset_to_zone(PROJECT_ID, PROJECT_NAME, zone_name, asset_name=bucket_name, bucket_name=bucket_name, service_account_key_path=SERVICE_ACCOUNT_KEY_PATH)

    save_to_bucket('tuberculosis_merged_FINAL.parquet', bucket_name, SERVICE_ACCOUNT_KEY_PATH)
    attach_asset_to_zone(PROJECT_ID, PROJECT_NAME, zone_name, asset_name=bucket_name, bucket_name=bucket_name, service_account_key_path=SERVICE_ACCOUNT_KEY_PATH)


    # save_to_bucket('tuberculosis_cases.csv','dataplexpoc-tuberculosis-cases-raw')
    # save_to_bucket('tuberculosis_outcomes.csv','dataplexpoc-tuberculosis-outcomes-raw')
    # save_to_bucket('tuberculosis_cases.parquet','dataplexpoc-tuberculosis-cases-final')
    # save_to_bucket('tuberculosis_outcomes.parquet','dataplexpoc-tuberculosis-outcomes-final')
    # save_to_bucket('tuberculosis_merged_FINAL.parquet','dataplexpoc-tuberculosis-merged-final')

# # #  UPLOAD ASSET (in terminal)
# export LAKE_NAME=infectious-diseases
# export PROJECT_ID=phx-01h41bw3b0xsf9rmpzmxbee2s9
# export LOCATION=us-central1

# export ZONE_NAME=tuberculosis-raw
# export ASSET_NAME=tuberculosis-cases-raw-table
# export BUCKET_NAME=dataplexpoc-tuberculosis-cases-raw

# export ZONE_NAME=tuberculosis-raw
# export ASSET_NAME=tuberculosis-outcomes-raw-table
# export BUCKET_NAME=dataplexpoc-tuberculosis-outcomes-raw

# export ZONE_NAME=tuberculosis-curated
# export ASSET_NAME=tuberculosis-outcomes-final-table
# export BUCKET_NAME=dataplexpoc-tuberculosis-outcomes-final

# export ZONE_NAME=tuberculosis-curated
# export ASSET_NAME=tuberculosis-cases-final-table
# export BUCKET_NAME=dataplexpoc-tuberculosis-cases-final

# export ZONE_NAME=tuberculosis-curated
# export ASSET_NAME=tuberculosis-merged-final-table
# export BUCKET_NAME=dataplexpoc-tuberculosis-merged-final


# gcloud dataplex assets create $ASSET_NAME \
# --project=$PROJECT_ID \
# --location=$LOCATION \
# --lake=$LAKE_NAME \
# --zone=$ZONE_NAME \
# --resource-type=STORAGE_BUCKET \
# --resource-name=projects/$PROJECT_ID/buckets/$BUCKET_NAME \
# --discovery-enabled 
 
