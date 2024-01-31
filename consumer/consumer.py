import yaml

from pymongo.mongo_client import MongoClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import DeserializingConsumer


def load_config():
    with open('config.yaml', 'r') as cfg:
        config = yaml.safe_load(cfg)
    return config


def connect_to_mongo(config):
    conn_url = config['connection_url']

    # Connect to MongoDB
    client = MongoClient(conn_url)

    db = client[config['database']]
    collection = db[config['collection']]
    return collection


def get_schema(config):
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

    # key and value deserializer
    key_deserializer = AvroDeserializer(schema_registry_client, key_schema_str)
    value_deserializer = AvroDeserializer(schema_registry_client, value_schema_str)
    return key_deserializer, value_deserializer


def create_consumer(config):
    key_deserializer, value_deserializer = get_schema(config)
    consumer = DeserializingConsumer(
        {
            'bootstrap.servers': config['kafka_bootstrap_server'],
            'security.protocol': config['security_protocol'],
            'sasl.mechanisms': config['sasl_mechanisms'],
            'sasl.username': config['sasl_username'],
            'sasl.password': config['sasl_password'],
            'key.deserializer': key_deserializer,
            'value.deserializer': value_deserializer,
            'group.id': config['group_id'],
            'auto.offset.reset': config['auto_offset_reset']
        }
    )

    # Subscribe to the 'product_updates' topic
    consumer.subscribe([config['kafka_topic']])
    return consumer


def consume_msg(consumer, collection):

    

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print('waiting for msg...')
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue
            
            print("Received message:", msg.value())
            # do data validation before inserting
            if 'BookingID' not in  msg.value() or  msg.value()['BookingID'] is None:
                print('Skip msg due to invalid BookingID')

            if not isinstance(msg.value()['BookingID'], str):
                print('Skip insert as booking id is not a string')

            # if not isinstance(msg.value()['BookingID'], str):
            #     print('Skip insert as booking id is not a string')

            # check if document already contains same booking Id
            existing_booking_id = collection.find_one({'BookingID':  msg.value()['BookingID']})
            
            if existing_booking_id:
                print(f"BookingID '{msg.value()['BookingID']}' already exists, skipping insertion")
            else:
                collection.insert_one(msg.value())
                print("Inserted message into MongoDB:", msg.value())


    except KeyboardInterrupt:
        pass
    finally:
        
        consumer.commit()
        consumer.close()
    #    client.close()


if __name__=="__main__":
    conf = load_config()
    consume = create_consumer(conf)
    conn = connect_to_mongo(conf)
    consume_msg(consume, conn)
