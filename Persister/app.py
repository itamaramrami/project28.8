from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
from Enricher.app import Enricher
import json



class Persister:
    def __init__(self):
        self.Enricher=Enricher()
        self.Enricher.fetch_producer()
        self.client = MongoClient("mongodb://localhost:27017")
        self.db = self.client["project28"]
        self.colA=self.db["tweets_antisemitic"]
        self.colB=self.db["tweets_not_antisemitic"]
        self.TOPIC_ANTISEMITIC = "tweets_antisemitic"
        self.TOPIC_NOT_ANTISEMITIC = "tweets_not_antisemitic"
        self.consumerA = KafkaConsumer(
            'enriched_preprocessed_tweets_antisemitic',
            auto_offset_reset='earliest',  
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self.consumerB = KafkaConsumer(
            'enriched_preprocessed_tweets_not_antisemitic',
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,  
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    

    def consumeA_messages(self,cunsumer,topic):
        for message in cunsumer:
            data = message.value
            data.pop("_id", None)
            self.db[topic].insert_one(data)



