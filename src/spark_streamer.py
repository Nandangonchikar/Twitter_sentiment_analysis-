from cProfile import label
from pyspark.streaming import StreamingContext
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, StringType, StructType, StructField
import json
from transformers import pipeline
import datetime
from elasticsearch import Elasticsearch


es = Elasticsearch(
    cloud_id="twitterStreamSentimentAnalysis:dXMtZWFzdC0yLmF3cy5lbGFzdGljLWNsb3VkLmNvbSQyZjhkNDRmMWQwMmY0MWY2OWUzNjJjYzAyNzkzYWRjNCRiODE2ZTk2MTI4ZjA0NDllYTRkNDg2YWM0NGE1Yzc0MQ=="
)


neg_tweet_count=0;
pos_tweet_count=0;
neu_tweet_count=0;
specific_model = pipeline(model="finiteautomata/bertweet-base-sentiment-analysis")

def do_sentiment_analysis(tweets):
        # print(tweets)
        sentiment=specific_model(tweets)   #sentiment analysis using huggingface transformers pipeline
        if(sentiment[0][label]=="NEG"):
                neg_tweet_count+=1
        elif(sentiment[0][label]=="NEU"):
                neu_tweet_count+=1
        elif(sentiment[0][label]=="POS"):
                pos_tweet_count+=1
        return json.dumps(sentiment);    

def process(df):
        udf_func = udf(lambda x: do_sentiment_analysis(x),returnType=StringType())
        df = df.withColumn("sentiment",lit(udf_func(df.value)))
        df = df.withColumn("timestamp",lit(lit(current_timestamp())))
        df = df.withColumn("date",to_date())
        # print(df.take(10))
        return df


spark = SparkSession\
        .builder\
        .appName("kafkaStreaming")\
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

lines_df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "twitter")\
        .load()\
        .selectExpr("CAST(value AS STRING)")


tweets_text_df = lines_df.select("value")
sentiment_data=process(tweets_text_df)


# parsed_df = lines_df \
#             .select(from_json(col('value').cast('string'),
#                     schema).alias('parsed_value')) \
#             .withColumn('timestamp', lit(current_timestamp()))
# print(type(lines_df))

# Schema = StructType([StructField("text", StringType(), True)])
# values=lines_df.select(from_json(lines_df.value.cast("string"), Schema).alias("test"))

#type 2
# tweets_text = lines_df.select("value")
# tweets_text_preprocessed=Preprocess_tweets(tweets_text)
# words = text_classification1(tweets_text)
# words = words.repartition(1)


query = sentiment_data\
        .writeStream\
        .format('console')\
        .outputMode('update')\
        .start()

query = sentiment_data\
        .writeStream\
        .format('org.elasticsearch.spark.sql')\
        .option('es.nodes', 'localhost')\
        .option('es.port', 9200)\
        .option('es.resource', '%s/%s' % ('assignment3', 'doc_type_name'))\
        .outputMode('update')\
        .start()
        
query.awaitTermination()


# ~/spark-3.3.0/spark-3.3.0-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:3.2.0 spark_streamer.py localhost:9092
