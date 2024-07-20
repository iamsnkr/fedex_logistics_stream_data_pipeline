import random
from datetime import datetime

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient  # make a connection to Schema registry
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import configparser as cf
from pymongo import MongoClient

"""
#Load the config properties 
"""
config = cf.ConfigParser()
config.read('config.properties', 'utf-8')

"""
# Connecting to MongoDB and Creating new database and collection
"""
conn_str = ("mongodb+srv://{}:{}@{}/?retryWrites=true&w=majority&appName=mydatabase"
            .format(config['MONGODB']['user.id'],
                    config['MONGODB']['user.id'],
                    config['MONGODB']['cluster']))
client = MongoClient(conn_str)
# Create or get the database 'fedex_logistics'
db = client.fedex_logistics
# Create or get collection 'shipment'
col = db.shipment
"""
# Connecting to schema registry and fetching latest avro schema details
"""
# Schema configurations
schema_reg_conf = {
    # URL
    'url': config['SCHEMA_REG_CONF']['url'],
    # APU Secret Key for the schema registry
    'basic.auth.user.info': config['SCHEMA_REG_CONF']['basic.auth.user.info']
}
# creating a schema registry client
schema_registry_client = SchemaRegistryClient(schema_reg_conf)
# Get the latest version of the schema
schema_str = (schema_registry_client
              .get_latest_version(config['SCHEMA_REG_CONF']['schema.name'])
              .schema
              .schema_str)

# Fetches the schema details and Deserializes the data based on that
value_avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
# It is just a String Deserializer
key_string_deserializer = StringDeserializer(codec='utf_8')

"""
# Connecting to Kafka Cluster and subscribing to the specific topic  
# Consuming messages from topic 
"""
# Kafka Consumer configurations
consumer_conf = {
    # Kafka Cluster Details
    'bootstrap.servers': config['KAFKA_CONF']['bootstrap.servers'],
    'sasl.mechanisms': config['KAFKA_CONF']['sasl.mechanisms'],
    'security.protocol': config['KAFKA_CONF']['security.protocol'],
    'sasl.username': config['KAFKA_CONF']['sasl.username'],
    'sasl.password': config['KAFKA_CONF']['sasl.password'],
    # Deserializer Details
    'key.deserializer': key_string_deserializer,  # Key will be deserialized as a string
    'value.deserializer': value_avro_deserializer,  # Value will be deserialized as Avro
    # Consumer Properties
    'group.id': config['CONSUMER_CONF']['group.id'],
    'auto.offset.reset': config['CONSUMER_CONF']['auto.offset.reset']
}


# Call back function for the consumer
def assign_callback(consumer, topic_partitions):
    print(f"topic_partitions : {topic_partitions}")
    for tp in topic_partitions:
        print(f"partition [{tp.partition}] from topic [{tp.topic}] at offset [{tp.offset}]")


# Creating an instance of the kafka consumer
consumer = DeserializingConsumer(consumer_conf)


def store_data_in_mongodb(msg):
    msg['timestamp'] = datetime.strptime(msg['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    return col.insert_one(msg).inserted_id


def consume_messages_from_kafka():
    consumer.subscribe(topics=[config['TOPIC_CONF']['topic.name']], on_assign=assign_callback)
    consumer_info = ("consumer id : '{}' , group : '{}'"
                     .format(random.randint(125058, 1255890), consumer_conf['group.id']))
    try:
        while True:
            msg = consumer.poll()
            if msg is None:
                continue
            if msg.error():
                print('Consumer error: {}'.format(msg.error()))
                continue
            try:
                _id = store_data_in_mongodb(msg.value())
                print(f"record inserted successfully into mongodb with id {_id}")
                print(
                    'Successfully consumed record with offset [{}] in partition [{}] from '
                    'topic [{}]'.format(msg.offset(), msg.partition(), msg.topic()))
                print("consumer_info \n message: {}".format(consumer_info, msg.value()))
                consumer.commit(msg)
            except Exception as e:
                print(f"Exception occurred while inserting data {e}")

        print("Messages are consumed successfully")
    except KeyboardInterrupt:
        pass
    finally:
        client.close()
        consumer.close()


if __name__ == "__main__":
    consume_messages_from_kafka()
