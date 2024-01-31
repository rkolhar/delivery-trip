This repo uses python to read csv data and Kafka to produce and consume messages. 
The consumer polls for the messages in Kafka cluster and writes them to Mongo DB document using Docker compose. 
The data stored in in Mongo DB collection can be further accessed using Fast API.

This code uses config.yaml to plug in configurations related to Mongo and Kafka.
