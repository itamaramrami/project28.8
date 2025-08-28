
from fastapi import FastAPI
from Persister.app import Persister


app = FastAPI()
cus=Persister()
cus.consumeA_messages(cus.consumerA,cus.TOPIC_ANTISEMITIC)
cus.consumeA_messages(cus.consumerB,cus.TOPIC_NOT_ANTISEMITIC)
   





@app.get("/")
def home():
    return {"message": "hello"}


@app.get("/messagesA")
def get_messages():
    return list(cus.colA.find({}, {'_id': 0}))
@app.get("/messagesB")
def get_messages():
    return list(cus.colB.find({}, {'_id': 0}))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="localhost", port=8001, reload=True)


