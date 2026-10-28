
# v6-omop-data-extraction

This algorithm extracts patient-level data from a specified cohort in an OMOP Common 
Data Model (CDM) database. It retrieves the variables defined in a configurable SQL 
template, applies basic data transformations, and returns the resulting dataset for use 
in downstream analytical workflows.

In the node, the following environment variables need to be set:

- `DATABASE_URI`: The URI of the OMOP database. This should be a valid OHDSI connection
  string. For example:
  ```
  jdbc:postgresql://localhost:5432/omop
  ```
- `DATABASE_TYPE`: The type of the database. This should be OMOP, but it will not be
  used in this algorithm.
- `DATABASE_USER`: The user of the database. This user only requires read access to the
  database.
- `DATABASE_PASSWORD`: The password of the database.

## Input parameters
The algorithm requires the following input parameter:

- `cohort_id`: Unique identifier of the target cohort in the OMOP database. 
The extraction process is restricted to patients belonging to the specified cohort, 
and only data associated with these individuals are included in the output dataset.

## Output
The algorithm returns a structured dataset containing the variables requested in the 
extraction query for all patients included in the target cohort.