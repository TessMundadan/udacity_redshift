#Context


Music streaming app wants to analyze the what songs users are listening to. Create a dimenson model and ETL pipeline to extract songs and user logging activity from JSON files, and load it to staging tables and then to dataware house in Redshift. 


#Database Schema Design

First we loaded the source files to staging tables, events and songs which are data dump of files<br>
We used a star schema.The dimension tables have details of each dimension. Fact table contain the ids of the dimension table and the metrics which are being analyzed<br>

Staging Tables<br>
staging_events<br>
staging_songs<br>

Dimension Tables<br>
users<br>
songs<br>
artists<br>
Time<br>

Fact table<br>
Songsplay


#ETL Pipeline Design

load_staging_tables()<br>
Reads JSON  files from the S3 and loads to staging tables in redshift.<br>

insert_tables()<br>
Inserts into dimension tables from staging tables.<br>


