# PURPOSE: 

This data lake has been set up to support easy-to-use optimized queries and data analysis for the Sparkify music streaming app, which stores all song and user activity data/metadata in JSON files. The AWS cloud-based ELT pipeline associated with this data lake uses AWS EMR/Spark to extract the Sparkify data from these JSON files (stored in S3) and load/transform it into a star schema of Parquet files stored in S3 for future use with analytical queries. The overall table schema is defined as follows: 

## Fact Table: 
fact_songplay : keys={songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent}

## Dimension Tables (4) 
dim_users : keys={user_id, first_name, last_name, gender, level}\
dim_songs : keys={song_id, title, artist_id, year, duration}\
dim_artists : keys={artist_id, name, location, latitude, longitude}\
dim_time : keys={start_time, hour, day, week, month, year, weekday}

The justifications for the choice of a star schema for the analytical queries include:

a) Being denormalized, the join logic required for queries is much simpler than with a normalized schema.\
b) Simplified reporting logic for queries of business interest.\
c) Optimized query performance (especially for aggregations).


# SCRIPTS/FILES PROVIDED

1) dl.cfg : Configuration file specifying AWS CREDENTIALS for the EMR cluster to be used
            in generating this data lake
             
(*NOTE: These credentials, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, must be generated on AWS and added to the file*)


2) etl.py : Using AWS credentials from 'dl.cfg', creates a Spark session on an AWS EMR cluster,
            extracts log data and song metadata JSON files from S3, loads and transforms the data into
            the required fact (songplays) and dimension tables (users, songs, artists, time) and stores
            them in S3 as Parquet files (with songs partitioned by year and then artist, and with
            times and songplays both partitioned by year and then month)
                      

# RUNNING THE PROVIDED SCRIPTS:

1) Create an EMR cluster on AWS (m5.xlarge x 3 is sufficient, with emr-5.28.0), download a .pem file 
   associated with it, and SSH into the cluster:

`ssh -i <pem-filename>.pem hadoop@<emr-cluster-master-public-dns>`

2) Copy dl.cfg and etl.py to the EMR cluster (into directory /home/hadoop):
    
`scp -i <pem-filename>.pem -v dl.cfg hadoop@<emr-cluster-master-public-dns>:/home/hadoop/`\
`scp -i <pem-filename>.pem -v etl.py hadoop@<emr-cluster-master-public-dns>:/home/hadoop/`

3) Use sudo and vi to append the line 'export PYSPARK_PYTHON=python3' to the end of 
   /etc/spark/conf/spark-env.sh:

`sudo vi /etc/spark/conf/spark-env.sh`\
(Go to end of file and type o, paste the new line, then type :wq)

4) Launch a spark job with etl.py to perform the ELT steps and generate the data lake:

`spark-submit etl.py`
