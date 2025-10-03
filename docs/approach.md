### Approach to Turbines Challenge

#### Bronze Layer

Summary:

The bronze pipeline ingests new raw data, casts the columns and saves to the bronze layer.
We aim to preserve data integrity and keep as close to the source data as possible

Assumptions:

- Missed entries:
  - From the brief, I am assuming that any missed entries will not be filled in later:
  - "Each day the csv will be updated with data from the last 24 hours, however the system is known to 
sometimes miss entries due to sensor malfunctions. "
  - Therefore I have set the bronze layer to check the latest timestamp of the bronze table if it exists. If the bronze table does exist the pipeline will ingest any data since the last set of entries ingested. If the bronze table does not exist it ingests all the data.
  - This allows scalability by maintaining the simplicity of the solution and avoids processing data every time, however it allows for recovery in the event of any pipeline failures.
- Missing values/outliers
  - Missing values and outliers are not removed in the bronze layer to preserve a record of the source and will be dealt with in the silver layer.

#### Silver Layer

Summary:
Cleaning and improving the usability of the data while avoiding complicated operations/aggregations and maintaining a record of what has been done

Assumptions:

- Filter out any nulls or any dates that are in the future
- Read only data that hasn't been loaded into the silver layer already
  - Carrying on the assumption from bronze that any missing data is never received, we only read data from bronze that is greater than the latest date in silver. Again this adds resilience in case of a failure but reduces memory usage.
- Impute data using pyspark imputer
  - We impute missing values to provide a full dataset, but add a binary `imputed` column to indicate which rows have been imputed in case end users would like to filter out this data.
- Partitioning
  - We partition the data by date for quicker queries on date-based aggregations

#### Gold Layer

Summary:
Contains useable, reliable, correct aggregations and summary stats

Assumptions:

- We calculate all summary stats in case of changes to ensure correctness of the stats and upsert into the delta table on primary keys
- We calculate the anomalies first and then remove them for the summary stats to avoid skewing the outputs
- Partitioning
  - We partition the data by date for quicker queries on date-based aggregations