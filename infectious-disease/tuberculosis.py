import random
from typing import List
from pathlib import Path

from utils import create_bucket, upload_blob
from options import health_unit, pt

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
        'Client DOB (Month/Year)': dt.formatted_date(fmt="%m/%Y", start=current_year-100, end=current_year-1),
        'Client Gender': generic.choice(["MALE", "FEMALE"]),
        'Client Death Date (Month/Year)': dt.formatted_date(fmt="%m/%Y", start=current_year-5, end=current_year-1),
        # 'Episode Death Cause Description': generic.choice(ep_death_cause_desc), 
        'Forward Sortation Area':  address.postal_code(),
        'Client Age at Time of Diagnosis': numeric.float_number(start=0.0, end=105.0, precision=1),
        'Episode Accurate Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode Start Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode Date Type': generic.choice(["ONSET", "SPECIMEN", "REPORTED"]),
        'TB Episode Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode External Reference Number': dt.formatted_date(fmt="%d-%m-%Y"),
        'Episode Type Description': generic.choice(["CASE", "CONTACT"]),
        # 'Episode Status Description': generic.choice(ep_status_desc),
        'Diagnosis Status Description': generic.choice(["CONFIRMED"]),
        'Diagnosis Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        # 'Episode Detection Method Description': generic.choice(ep_detect_method_desc),
        'Diagnosing Health Unit Area Description': generic.choice(health_unit),
        'Responsible Health Unit Area Description': generic.choice(health_unit),
        'Client Birth Province': address.province(),
        # 'Client Origin': generic.choice(origin),
        "First Nation's Status": generic.choice(["ON RESERVE MOST OF TIME - NO", "ON RESERVE MOST OF TIME - YES"]),
        'Client Immigration Country From': address.country(),
        'Client Immigration Country Last Reside': address.country(),
        'Client Immigration Birth Country': address.country(),
        'Client Immigration Father Birth Country': address.country(),
        'Client Immigration Mother Birth Country': address.country(),
        'Client Immigration Country Father Last Reside': address.country(),
        'Client Immigration Country Mother Last Reside': address.country(),
        'Client Immigration Arrive Date':  dt.formatted_date(fmt="%d-%m-%Y", start=current_year-20, end=current_year-1),
        # 'Client Immigration Status at Arrival': generic.choice(imm_status_at_arr),  
        'Client Immigration Reported Surveillance Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode Updates Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-2, end=current_year-1), 
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

        'Client DOB (Month/Year)': dt.formatted_date(fmt="%m/%Y", start=current_year-100, end=current_year-1),
        'Client Gender': generic.choice(["MALE", "FEMALE"]),
        'Client Death Date (Month/Year)': dt.formatted_date(fmt="%m/%Y", start=current_year-5, end=current_year-1),
        # 'Episode Death Cause Description': generic.choice(ep_death_cause_desc), 
        'Forward Sortation Area':  address.postal_code(),
        'Client Age at Time of Diagnosis': numeric.float_number(start=0.0, end=105.0, precision=1),
        'Episode Accurate Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode Start Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode Date Type': generic.choice(["ONSET", "SPECIMEN", "REPORTED"]),
        'TB Episode Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode External Reference Number': dt.formatted_date(fmt="%d-%m-%Y"),
        'Episode Type Description': generic.choice(["CASE", "CONTACT"]),
        # 'Episode Status Description': generic.choice(ep_status_desc),
        'Diagnosis Status Description': generic.choice(["CONFIRMED"]),
        'Diagnosis Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        # 'Episode Detection Method Description': generic.choice(ep_detect_method_desc),
        'Diagnosing Health Unit Area Description': generic.choice(health_unit),
        'Responsible Health Unit Area Description': generic.choice(health_unit),
        'Client Birth Province': address.province(),
        # 'Client Origin': generic.choice(origin),
        "First Nation's Status": generic.choice(["ON RESERVE MOST OF TIME - NO", "ON RESERVE MOST OF TIME - YES"]),
        'Client Immigration Country From': address.country(),
        'Client Immigration Country Last Reside': address.country(),
        'Client Immigration Birth Country': address.country(),
        'Client Immigration Father Birth Country': address.country(),
        'Client Immigration Mother Birth Country': address.country(),
        'Client Immigration Country Father Last Reside': address.country(),
        'Client Immigration Country Mother Last Reside': address.country(),
        'Client Immigration Arrive Date':  dt.formatted_date(fmt="%d-%m-%Y", start=current_year-20, end=current_year-1),
        # 'Client Immigration Status at Arrival': generic.choice(imm_status_at_arr),  
        'Client Immigration Reported Surveillance Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-5, end=current_year-1),
        'Episode Updates Date': dt.formatted_date(fmt="%d-%m-%Y", start=current_year-2, end=current_year-1), 
    }
)

if __name__ == "__main__":
    tuberculosis_outcomes_schema = episode_info_fields(1000, 2023)
    df = pd.DataFrame(tuberculosis_outcomes_schema.create())
    df.to_parquet("./data/tuberculosis_outcomes.parquet", engine="pyarrow")

    tuberculosis_cases_schema = lab_extract_fields(1000, 2023)
    df = pd.DataFrame(tuberculosis_cases_schema.create())
    df.to_csv("./data/tuberculosis_cases.csv", index=False)

    tuberculosis_merged_schema = merged_fields(1000, 2023)
    df = pd.DataFrame(tuberculosis_merged_schema.create())
    df.to_parquet("./data/tuberculosis_merged_FINAL.parquet", engine="pyarrow")

    bucket_name = 'phx-dp-tuberculosis'
    create_bucket(bucket_name)

    source_file_name = './data/tuberculosis_outcomes.parquet'
    destination_blob_name = 'tuberculosis_outcomes.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = './data/tuberculosis_cases.csv'
    destination_blob_name = 'tuberculosis_cases.csv'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

    source_file_name = './data/tuberculosis_merged_FINAL.parquet'
    destination_blob_name = 'tuberculosis_merged_FINAL.parquet'
    upload_blob(bucket_name, source_file_name, destination_blob_name)

