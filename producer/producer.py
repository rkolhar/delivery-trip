import pandas as pd
import numpy as np
import yaml

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


def load_config():
    with open('config.yaml', 'r') as cfg:
        config = yaml.safe_load(cfg)
    return config


def get_schema(config: dict):
    # create schema for schema registry client
    schema_registry_client = SchemaRegistryClient({   
                'url': config['schema_registry_url'], 
                'basic.auth.user.info': f"{config['auth_user']}:{config['auth_pass']}"
            })

    # avro value schema
    value_subject_name = f"{config['kafka_topic']}-value"
    value_schema_str = schema_registry_client.get_latest_version(value_subject_name).schema.schema_str

    # avro key schema
    key_subject_name = f"{config['kafka_topic']}-key"
    key_schema_str = schema_registry_client.get_latest_version(key_subject_name).schema.schema_str

    # key and value serializer
    key_serializer = AvroSerializer(schema_registry_client, key_schema_str)
    value_serializer = AvroSerializer(schema_registry_client, value_schema_str)

    return key_serializer, value_serializer


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print(f'record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset { msg.offset()}')
    

def create_producer(config: dict):
    key_serializer, value_serializer = get_schema(config)
    producer = SerializingProducer({
      'bootstrap.servers' : config['kafka_bootstrap_server'],
      'sasl.mechanisms' : config['sasl_mechanisms'],
      'security.protocol' : config['security_protocol'],
      'sasl.username' : config['sasl_username'],
      'sasl.password' : config['sasl_password'],
      'key.serializer' : key_serializer,
      'value.serializer' : value_serializer
    })
    return producer


def read_csv():

    input_file = 'delivery_trip_truck_data.csv'
    columns_to_str = ['MaterialShipped', 'Driver_MobileNo', 'customerID', 'vehicleType', 'trip_end_date', 'ontime', 'Driver_Name']
    df = pd.read_csv(input_file, header=0, dtype=dict.fromkeys(columns_to_str, str))
    #df.replace('NULL', 'NA', inplace=True)
    df.fillna('Unknown', inplace=True) 
    df.rename(columns={'Market/Regular ': 'Market_or_Regular'}, inplace=True)

    return df


def send_msg(producer: dict, config: dict, df):

    for ind, row in df.iterrows():
        val = row.to_dict()
        producer.produce(topic = config['kafka_topic'], key=str(val['BookingID']), value=val, on_delivery=delivery_report)
        producer.flush()
    
    print('Msgs sent to kafka broker successfully')


if __name__=="__main__":
    conf = load_config()
    prod = create_producer(conf)
    read_data = read_csv()
    send_msg(prod, conf, read_data)