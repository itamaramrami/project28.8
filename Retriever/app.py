from pymongo import MongoClient
from kafka import KafkaProducer
import json
import time
import datetime
from bson import ObjectId


class DataLoader:
    def __init__(self):
        self.uri = "mongodb+srv://IRGC_NEW:iran135@cluster0.6ycjkak.mongodb.net/"
        self.client = MongoClient(self.uri)
        self.db = self.client.IranMalDB
        col_names = self.db.list_collection_names()
        self.collection = self.db[col_names[0]]
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        self.TOPIC_ANTISEMITIC = "raw_tweets_antisemitic"
        self.TOPIC_NOT_ANTISEMITIC = "raw_tweets_not_antisemitic"


    def serialize_doc(self, doc):
            return {
            k: (str(v) if isinstance(v, ObjectId) else v.isoformat() if isinstance(v, datetime.datetime) else v)
            for k, v in doc.items()}



    def fetch_docs_after(self,limit=10):
        cursor = (self.collection.find().limit(limit))
        return list(cursor)

    def fetch_and_publish(self):
        # while True:
            docs = self.fetch_docs_after()
            if docs:
                for doc in docs:
                    if doc["Antisemitic"]=="1":
                        topic=self.TOPIC_ANTISEMITIC
                    else:
                        topic=self.TOPIC_NOT_ANTISEMITIC
                    self.producer.send(topic,self.serialize_doc(doc))
                self.producer.flush()
                self.last_sent_timestamp = max(doc['CreateDate'] for doc in docs)
                print(f"Sent {len(docs)} messages to Kafka")
            else:
                print("No new messages to send")
            # time.sleep(60)
            return
