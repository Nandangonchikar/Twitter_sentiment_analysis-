Sentiment analysis of twitter stream 

![alt text](https://github.com/[nandangonchikar]/[Twitter_sentiment_analysis-]/blob/[master]/images/Flowchart.png?raw=true)

Dependencies:
Java - openjdk 11.0.15 2022-04-19
kafka_2.12-3.2.0
spark-3.3.0
Spark-streaming-kafka-0-8_2.11:3.3.0
elasticsearch-hadoop-5.6.4

Python packages:
transformers
ElasticSearch
kafka
tweepy
pyspark
JSOn
datetime

Sentiment Analysis:
	-Tweets are classified as Positive, Negative or Neutral along with their scores.
	-Higher the score[0>score<1], more is the efficiency of classification

- This project is done on WSL on windows.

How to run:
-start zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
-start kafka: bin/kafka-server-start.sh config/server.properties
-created a topic called "twitter"
-Run "producertwitter.py", which streams data from twitter, clean, build to a JSON message and send it to the kafka cluster.
		-uses TwitterV2 API for data streaming.
		-can set the filter topic by mentioning in the variable "filterTopic"
		-If we want to delete the filter topics previously set, run the function remove_topics(ruleID)
						-remove_topics(obj_ID) -> Inside the function, the previously set ruleIDs are obtained and deleted through API endpoints.
		
-Run spark-streamer.py, which will connect to the topic "twitter".
		-Extract the text message from the recieved message
		-Used Huggingface pre trained library to perform sentiment analysis on the twitter data.
		-pass the sentiment data to elasticsearch through the dataframe.
-Run elasticSearch_consumer.py, which establishes a connection between the elastic search and kafka.


ELK stack:
-Launch elasticsearch, kibana and logstash by going inside their installation direcories.
-In kibana, we can visually see the sentiment metrics of the data,change the timeline..
