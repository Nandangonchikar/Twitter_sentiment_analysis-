from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from datetime import datetime
from uuid import uuid4
es = Elasticsearch(
    cloud_id="twitterStreamSentimentAnalysis:dXMtZWFzdC0yLmF3cy5lbGFzdGljLWNsb3VkLmNvbSQyZjhkNDRmMWQwMmY0MWY2OWUzNjJjYzAyNzkzYWRjNCRiODE2ZTk2MTI4ZjA0NDllYTRkNDg2YWM0NGE1Yzc0MQ=="
)
es.index(index="assignment3", id=42, document={"any": "data", "timestamp": datetime.now()})