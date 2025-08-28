
@REM הרצת קונטינר mongodb
docker run --name mongodb -p 27017:27017 -d mongodb/mongodb-community-server:latest



@REM בניית Img ודחיפה docker hub
docker build -t itamar12/mmongodb-crud-server:latest .
docker push itamar12/mongodb-crud-server:latest

./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic


@REM מחיקת תוכן הפרוייקט
oc delete all --all
oc delete pvc --all



בניית פוד עם servis וpvc



oc apply -f infra/deployment_fastapi.yaml
oc apply -f infra/service_fastapi.yaml
oc apply -f infra/route.yaml 



