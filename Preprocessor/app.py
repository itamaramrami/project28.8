from kafka import KafkaProducer, KafkaConsumer
from Retriever.app import DataLoader
import json
import string



class Preprocessor:
    def __init__(self):
        self.data=DataLoader()
        self.data.fetch_and_publish()
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        self.TOPIC_ANTISEMITIC = "preprocessed_tweets_antisemitic"
        self.TOPIC_NOT_ANTISEMITIC = "preprocessed_tweets_not_antisemitic"
        self.consumerA = KafkaConsumer(
            'raw_tweets_antisemitic',
            auto_offset_reset='earliest',  
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self.consumerB = KafkaConsumer(
            'raw_tweets_not_antisemitic',
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,  
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self.stop_words=["a", "an", "the", "and", "or", "but", "is", "are", "I", "you"]

    
    def data_processing(self,text):
        text=text.lower()
        text= "".join(ch for ch in text if ch not in string.punctuation)
        text=" ".join(text.split())
        text=[w for w in text.split() if w.lower() not in self.stop_words]
        return " ".join(text)


    def get_data_for_consumer(self, consumer, max_messages=10):
        list_of_consumer = []

        for i, msg in enumerate(consumer):
            if i >= max_messages:
                break
            data = msg.value
            data["clean_text"] = self.data_processing(data["text"])
            list_of_consumer.append(data)
        return list_of_consumer
    
    def fetch_producer(self):
            docsA = self.get_data_for_consumer(self.consumerA)
            docsB = self.get_data_for_consumer(self.consumerB)
            for doc in docsA:
                    self.producer.send(self.TOPIC_ANTISEMITIC,self.data.serialize_doc(doc))
                    self.producer.flush()
            print(f"Sent {len(docsA)} messages to Kafka in consoumerA")
            
            for doc in docsB:
                    self.producer.send(self.TOPIC_NOT_ANTISEMITIC,self.data.serialize_doc(doc))
                    self.producer.flush()
            print(f"Sent {len(docsB)} messages to Kafka in consoumerB")
            return
           
    


