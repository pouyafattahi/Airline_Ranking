# Airline Ranking

This is the project to rank some airlines based on twitts which are about those. This project is implemented by python and uses spark.

## Dataset files can ben found at 

http://www.sfu.ca/~alopes/732_project/dataset.zip 

## Report and results at

http://www.sfu.ca/~vpalenge

## Instructions to run Daemon

cd tweet_stream 
The command is running and saving the twitters inside the folder 'airlines_tweets'!

### Copy the files to be processed

hdfs dfs -copyFromLocal  airlines_tweets 
back to the main directory: cd .. 
copying the training file: hdfs dfs -copyFromLocal airline.csv

## Executing
spark-submit --master=yarn-client --executor-memory 6G --num-executors 15 --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --py-files svm.zip process_twitter.py airlines_tweets airline.csv 







