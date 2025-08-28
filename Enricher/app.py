from kafka import KafkaProducer, KafkaConsumer
from Preprocessor.app import Preprocessor
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json
from bson import ObjectId
import datetime




class Enricher:
    def __init__(self):
        
        self.preprocessor=Preprocessor()
        self.preprocessor.fetch_producer()
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        self.TOPIC_ANTISEMITIC = "enriched_preprocessed_tweets_antisemitic"
        self.TOPIC_NOT_ANTISEMITIC = "enriched_preprocessed_tweets_not_antisemitic"
        self.consumerA = KafkaConsumer(
            'preprocessed_tweets_antisemitic',
            auto_offset_reset='earliest',  
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self.consumerB = KafkaConsumer(
            'preprocessed_tweets_not_antisemitic',
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,  
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    
    
    def red_txt(self):
        with open(r"C:\Users\IMOE001\Desktop\project\weapons\weapon_list.txt", 'r') as file:
            self.weapons = [w.strip() for w in file.read().split(",")]
        return self.weapons
    
    def serialize_doc(self, doc):
            return {
            k: (str(v) if isinstance(v, ObjectId) else v.isoformat() if isinstance(v, datetime.datetime) else v)
            for k, v in doc.items()}
    
    def Emotion_test(self,text):
        self.score=SentimentIntensityAnalyzer().polarity_scores(text)
        if self.score["compound"]>0.5 and self.score["compound"]<1:
            return "Positive text"
        if self.score["compound"]>0 and self.score["compound"]<0.5:
            return "Neutral  text"
        if self.score["compound"]>-1 and self.score["compound"]<-0.5:
            return "Negative  text"
        else:
            return "no emotion"
    
    def list_of_text(self, text):
        return text.lower().split()

    def Weapon_inspection(self,text):
        self.weapon=self.red_txt()
        self.text=self.list_of_text(text)
        for i in self.text:
            if i in self.weapon:
                return i
            else:
                return " "   
    def get_data_for_consumer(self, consumer, max_messages=10):
        list_of_consumer = []

        for i, msg in enumerate(consumer):
            if i >= max_messages:
                break
            data = msg.value
            data["sentiment"] = self.Emotion_test(data["text"])
            data["text"] = self.preprocessor.data_processing(data["text"])
            data["weapons_detected"] = self.Weapon_inspection(data["text"])
            list_of_consumer.append(data)
        return list_of_consumer
    

    def fetch_producer(self):
            docsA = self.get_data_for_consumer(self.consumerA)
            docsB = self.get_data_for_consumer(self.consumerB)
            for doc in docsA:
                    self.producer.send(self.TOPIC_ANTISEMITIC,self.serialize_doc(doc))
                    self.producer.flush()
            print(f"Sent {len(docsA)} messages to Kafka in consoumerA")
            
            for doc in docsB:
                    self.producer.send(self.TOPIC_NOT_ANTISEMITIC,self.serialize_doc(doc))
                    self.producer.flush()
            print(f"Sent {len(docsB)} messages to Kafka in consoumerB")
            return
    

