from confluent_kafka import SerializingProducer  # To Serialize Data Class
from confluent_kafka.schema_registry import SchemaRegistryClient  # make a connection to Schema registry
from confluent_kafka.schema_registry.avro import AvroSerializer  # To serialize value data to avro
from confluent_kafka.serialization import StringSerializer  # To serialize key data
from configparser import ConfigParser
import configparser as cf
from datetime import datetime
from data_generator.logistics_data_generator import generate_logistics_data

# Load the config properties
config = cf.ConfigParser()
config.read('config.properties', 'utf-8')


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}"
              .format(msg.key(), err))
    else:
        print("User record {} successfully produced to {} [{}] at offset {}"
              .format(msg.key(), msg.topic(), msg.partition(), msg.offset()))
        print("Message value is {}".format(msg.value()))


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

# Fetches the schema details and serializes the data based on that
value_avro_serializer = AvroSerializer(schema_registry_client, schema_str)
# It is just a String Serializer
key_string_serializer = StringSerializer('utf_8')

# Kafka Producer configurations
producer_conf = {
    # Kafka Cluster Details
    'bootstrap.servers': config['KAFKA_CONF']['bootstrap.servers'],
    'sasl.mechanisms': config['KAFKA_CONF']['sasl.mechanisms'],
    'security.protocol': config['KAFKA_CONF']['security.protocol'],
    'sasl.username': config['KAFKA_CONF']['sasl.username'],
    'sasl.password': config['KAFKA_CONF']['sasl.password'],
    # Serializer Details
    'key.serializer': key_string_serializer,  # Key will be serialized as a string
    'value.serializer': value_avro_serializer  # Value will be serialized as Avro
}

# Creating an instance of the kafka producer
producer = SerializingProducer(producer_conf)


def produce_messages_to_kafka():
    # dict_value['timestamp'] = datetime.strftime(dict_value['timestamp'], "%Y-%m-%dT%H:%M:%SZ")
    for _ in range(1750):
        dict_value = generate_logistics_data()
        producer.produce(topic=config['TOPIC_CONF']['topic.name'],
                         key=str(dict_value['shipment_id']),
                         value=dict_value,
                         on_delivery=delivery_report)
        producer.flush()
    print("Messages are produces successfully")


if __name__ == "__main__":
    produce_messages_to_kafka()
