# Dataplex PoC

[Dataplex](https://cloud.google.com/blog/products/data-analytics/build-a-data-mesh-on-google-cloud-with-dataplex-now-generally-available?utm_source=youtube&utm_medium=unpaidsoc&utm_campaign=fy22q1-googlecloudevents-blog-data-description-no-brand-global&utm_content=j2hU_vkiWa0-skyvine1026739764&utm_term=-) is a GCP datamesh service.  With Dataplex, data products are decentraly owned and managed by the buisness domains (prgram areas) and stored in Google Cloud Storage Buckets and/or Big Query. Metadata can be extracted and searched in a central location (findability). We can add data quality tasks ensure data contracts are adhered to and limit or enable access with IAM groups. 


In this proof of concept project, we're exploring 
- Data standardization for encoding vs  well documented data contract (metadata?)
- different fomats (parq, csv), locations bq, gcs
- access - user authentication and how does that pan out for access - work flow
- querying across data products


## Set up

1. [Create Lakes](https://cloud.google.com/dataplex/docs/create-lake) 
    *  Lakes maps to a Data Mesh domain - here they correspond to PHAC branches (need a separate metastore per lake if using the explore features)
    * [Set up a Dataproc metastore service](https://cloud.google.com/dataplex/docs/create-lake#metastore) (select 'sync to data catalog' and enable grpc)

2. [Create Zones](https://cloud.google.com/dataplex/docs/add-zone) within lakes - these correspond to Surveillance Program areas. 
    * There are 2 tiers - raw zones (any format), and curated zones (strict typed format - eg BQ table/ parquet).
    * Enable [metadata discovery](https://cloud.google.com/dataplex/docs/add-zone#:~:text=the%20same%20zone.-,Optional,-%3A%20Enable%20metadata%20discovery) (allows Dataplex to automatically scan and extract metadata from Zone)

3. Add data assets to zones 
    * If adding from bucket, bucket location needs to be same as lake/zone.
    * Attach data assets from other projects: add [IAM to bucket](https://cloud.google.com/dataplex/docs/manage-assets#role-for-bucket) and [authorize bucket to dataplex](https://cloud.google.com/dataplex/docs/create-lake#access-control)
    * Flag managed (if looking for  find grain security https://cloud.google.com/dataplex/docs/manage-assets#upgrade-asset )


4. Table Entities
    * metadata
    * note - BQ table is added with metadata when using metastore


## To use 

<!-- Search for [data](https://cloud.google.com/data-catalog/docs/how-to/search) -->


## Working Notes

Characters
* Managed and/or curated datasets require field name characters be in the set 0-9, _,  a-z, A-Z - no spaces, brackets or hyphens are accepted. 
* Date values cannot have '/' - replace with hyphen. 

* If discovery is turned on for a zone, data will be scanned when added. Can also set cron jobs to scan.
* When assets are added to a Zone, a big query table is added to store the metadata

<!-- pricing https://cloud.google.com/dataplex/pricing#dataplex_premium_processing_pricing -->

### Working Resources

* [dataplex in 3 parts medium article](https://medium.com/search?q=Diptiman+Raichaudhuri+dataplex)
* [Google -Build a datamesh tutorial](https://cloud.google.com/dataplex/docs/build-a-data-mesh?utm_source=youtube&utm_medium=unpaidsoc&utm_campaign=fy22q1-googlecloudevents-web-data-description-no-brand-global&utm_content=j2hU_vkiWa0-skyvine1026739764&utm_term=-)
* [Youtube overview](https://www.youtube.com/watch?v=j2hU_vkiWa0&t=970s)


<!-- service-176013304796@gcp-sa-dataplex.iam.gserviceaccount.com -->





