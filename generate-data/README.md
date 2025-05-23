# Sample Synthetic Data

The focus here is to generate data to demonstrate Dataplex's functionality. While we're using the surveillance programs names, the data generated is based on over-simplfied, non-epidemiological guestimates - please treat these more as placeholders.  

## Update

This has been re-worked to have one survellance activity per GCP project as Dataplex is active on the global plane. Only 4 of these surveilance programs are being used, across 4 GCP projects:

* Lyme disease
* Tuberculosis
* seasonal influenza vaccination coverage
* survey of vaccination during pregnancy 

1. Create and populate .env file
2. Generate service account keys for each of the projects's service accounts for storage, save at the root.
3. Activate virtual environment
4. pip install -r generate-data/requirements.txt
5. Run files prefixed with 'p': p1 -> p4 to create fake data, populate buckets and attach data assets to zones. 

-------------

## Branches and Surveillance Programs

### NML 
* covid 19 wastewater

### Vaccine Roll-out Task force
* seasonal influenza vaccination coverage
* survey of vaccination during pregnancy 

### Infectious Diseases Program Branch (with NML)
* Flu watch / Flu watchers

### Infectious Diseases Program Branch
* Lyme disease
* Tuberculosis

### Health promotion and chronic disease prevention branch 
* Cancer in young people

### Strategic policy branch 
* Health Inequalities Reporting inititive


