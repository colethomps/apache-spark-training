 launch the spark and iceberg docker containers:
make up

Then, access a Jupyter notebook at localhost:8888.

The first notebook to be able to run is the event_data_pyspark.ipynb inside the notebooks folder.

I will be using the following data model in Spark

- match_details
  - a row for every players performance in a match
- matches
  - a row for every match 
- medals_matches_players 
  - a row for every medal type a player gets in a match
- medals
  - a row for every medal type 


My goal is to make the following things happen:

- Build a Spark job that
  - Disabled automatic broadcast join with `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
  - Explicitly broadcast JOINs `medals` and `maps`
  - Bucket join `match_details`, `matches`, and `medal_matches_players` on `match_id` with `16` buckets
  - Aggregate the joined data frame to figure out questions like:
    - Which player averages the most kills per game?
    - Which playlist gets played the most?
    - Which map gets played the most?
    - Which map do players get the most Killing Spree medals on?
  - With the aggregated data set
    - Try different `.sortWithinPartitions` to see which has the smallest data size (hint: playlists and maps are both very low cardinality)
    