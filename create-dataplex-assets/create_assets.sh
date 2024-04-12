#!/bin/bash
#  Reference - https://medium.com/google-cloud/google-cloud-dataplex-part-2-dataplex-gcloud-cli-819d3c678518

PROJECT_ID=phx-01h41bw3b0xsf9rmpzmxbee2s9
LOCATION=us-central1

# authenticate and set project 
gcloud auth login 
gcloud config set project $PROJECT_ID


# # Enable services and big query api
# gcloud services enable metastore.googleapis.com
# gcloud services enable dataplex.googleapis.com
# gcloud services enable bigquery.googleapis.com
# gcloud services enable bigquery.googleapis.com --project=$PROJECT_ID


# Dataproc Metastore 
DATAPROC_METASTORE_NAME=[metastore-for-hpcd, metastore-for-vrotf, metastore-for-spb, 'metastore-for-nml', 'metastore-for-id-nml']
LAKE_NAME=['health-promotion-and-chronic-disease-prevention', 'vaccine-roll-out-task-force', strategic-policy-branch, national-microbiology-lab, infectious-nml-partnership
LAKE_DISPLAY_NAME=['Health Promotion and Chronic Disease Prevention', 'Vaccine Roll-out Task Force', 'Strategic Policy Branch', 'National Microbiology Lab', 'Infectious Diseases NML Partnership']

export PROJECT_ID=phx-01h41bw3b0xsf9rmpzmxbee2s9
export LOCATION=us-central1
export DATAPROC_METASTORE_NAME=metastore-for-id-nml
export DATAPROC_METASTORE_NAME=metastore-for-spb
export DATAPROC_METASTORE_NAME=metastore-for-vrotf
export DATAPROC_METASTORE_NAME=metastore-for-nml
export DATAPROC_METASTORE_NAME=metastore-for-hpcd

gcloud beta metastore services create $DATAPROC_METASTORE_NAME \
--location=$LOCATION \
--hive-metastore-version=3.1.2 \
--endpoint-protocol=GRPC


#  confirm metastore created correctly 
gcloud metastore services describe $DATAPROC_METASTORE_NAME \
--project $PROJECT_ID \
--location $LOCATION \
--format "value(endpointUri)"


# # Create dataplex lake 
export LAKE_NAME='health-promotion-and-chronic-disease-prevention'
export LAKE_DISPLAY_NAME='Health Promotion and Chronic Disease Prevention'

# gcloud dataplex lakes create $LAKE_NAME \
# --project $PROJECT_ID \
# --location=us-central1 \
# --metastore-service=projects/$PROJECT_ID/locations/us-central1/services/$DATAPROC_METASTORE_NAME \
# --display-name=$LAKE_DISPLAY_NAME

gcloud alpha dataplex lakes create $LAKE_NAME \
 --location=$LOCATION \
 --metastore-service=projects/$PROJECT_ID/locations/us-central1/services/$DATAPROC_METASTORE_NAME \
#  --display-name=$LAKE_DISPLAY_NAME


# cron job scheduler https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules

# Create zone 
export PROJECT_ID=phx-01h41bw3b0xsf9rmpzmxbee2s9
export LOCATION=us-central1
export ZONE_NAME= 'Health Inequalities Health Inititive Raw'
export LAKE_NAME=strategic-policy-branch

gcloud dataplex zones create $ZONE_NAME \
--lake=$LAKE_NAME \
--resource-location-type=SINGLE_REGION \
--location=us-central1 \
--project=$PROJECT_ID \
--resource-location-type=SINGLE_REGION \
--discovery-enabled \
--type=RAW
# --discovery-schedule="0 0 * * *" \

# --type=CURATED
# --labels=data_product_category=raw_data

# Add gcs assets to zone
export ASSET_NAME=
export BUCKET_NAME=

gcloud dataplex assets create $ASSET_NAME \
--project=$PROJECT_ID \
--location=$LOCATION \
--lake=$LAKE_NAME \
--zone=$ZONE_NAME \
--resource-type=STORAGE_BUCKET \
--resource-name=projects/$PROJECT_ID/buckets/$BUCKET_NAME \
--discovery-enabled \
--discovery-schedule="0 0 * * *" \
--csv-delimeter="," #\
# --csv-header-rows=1


# Add bq assets to zone 
ASSET_NAME=
BQ_DATASET_NAME=

gcloud dataplex assets create $ASSET_NAME \
--project=$PROJECT_ID \
--location=us-central1 \
--lake=$LAKE_NAME \
--zone=$ZONE_NAME \
--resource-type=BIGQUERY_DATASET \
--resource-name=projects/$PROJECT_ID/datasets/$BQ_DATASET_NAME \
--discovery-enabled 