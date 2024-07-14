# Sparkify ETL Pipeline
#### Ian McDonough


## AWS Project Aims
The aim of this project is to build a scalable ETL pipeline which will take app data from the media app "Sparkify", which are currently stored in S3, and convert them into 5 normalized tables stored in Amazon Redshift. The choice to store this data in Third Normal Form (3NF) on Amazon Redshift will enable data analysis while making efficient use of storage resources.


# Files And How to Run Them
This zipped folder contains four additional files necessary to run the pipeline:
1) 'sql_queries.py': Defines SQL queries which will be used to create the empty tables and populates them with the Sparkify data
2) 'create_tables.py': Creates the tables by executing the SQL queries in sql_queries.py
3) 'etl.py': Executes the SQL queries that load the data into the created tables on an AWS cluster
4) 'dwh.cfg': Which contains the credentials and addresses necessary to access the AWS server and Sparkify data including HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, ARN, LOG_DATA, LOG_JSONPATH, and SONG_DATA


To execute this pipeline you must:
1) Insert your personal AWS data, from an active cluster in the us-west-2 region, into the dwh.cfg file and save it in the same file which contains the other files listed above
2) From a terminal first execute the 'sql_queries.py' file
3) Then execute the 'create_tables.py' file
4) Lastly execute the 'etl.py' file
5) Allow for several minutes for the data transfer to complete
6) Confirm the completion of the ETL pipeline from your AWS account

# Attributions
Udacity Mentor Survesh: https://knowledge.udacity.com/questions/1041967

