# Delivery-trip

This repo uses python to read csv data and Kafka to produce and consume messages. 
The consumer polls for the messages in Kafka cluster and writes them to Mongo DB document using Docker compose. 
The data stored in in Mongo DB collection can be further accessed using Fast API.

This code uses config.yaml to plug in configurations related to Mongo and Kafka.

## Steps to execute code:
1. Spin up docker containers using cmd:
```bash
    docker compose up –d –build
```
2. Check data in mongo db
   
3. To create an API, navigate to app folder and install requirements required using:
   ```bash
   pip install –r requirements.txt
   ```
  
4. To run the api use in terminal:
```bash
   uvicorn api:app –reload
```
5. Browse http://localhost:8000/docs to access Swagger UI to interact with queries.
