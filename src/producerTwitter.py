from datetime import datetime
import tweepy ;
import kafka;
import json
import datetime




filterTopic="#marvel lang:en"   ##set the topic here to read specific data from filter
BEARER_TOKEN='AAAAAAAAAAAAAAAAAAAAAPcufAEAAAAAxKKd6R0QW0CVnB%2BXK%2Br5b1HT730%3DxPo5x5EToJU0TxFLwMFlpQPfQmUqMSYCltL3yUlraRxgVoWB0J'

  ##Delete all the rules created from beginning. If the filter topic is changed from a to b then both will be applied
  #run this function to remove all the rules

def remove_topics(printer):  
	applied_rules=printer.get_rules()
	printer.delete_rules(applied_rules)

def clean_data(data):
	raw_tweet=json.loads(data)
	tweet={}
	tweet["text"] = raw_tweet['data']['text']
	tweet["id"] = raw_tweet["data"]["id"]
	return json.dumps(tweet)

class IDPrinter(tweepy.StreamingClient):
	def on_data(self, tweet):
		data_json=json.loads(tweet);
		# print(data_json)
		twitter_text=data_json['data']['text'].encode('utf-8')
		print(twitter_text)
		producer.send("twitter",twitter_text)
		return True
      

	def on_errors(self, errors):
		print(f"Received error code {errors}")
		self.disconnect()
		return False


producer=kafka.KafkaProducer(bootstrap_servers='localhost:9092') #,value_serializer=lambda m: json.dumps(m).encode('utf-8')
printer = IDPrinter(BEARER_TOKEN,return_type=dict,wait_on_rate_limit=True)
printer.add_rules(tweepy.StreamRule(filterTopic))    
printer.filter()






