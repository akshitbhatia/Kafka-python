from pykafka import KafkaClient
from utils import database
import config
import json

def TwitterFollowersConsumer(topic, broker, consumer_group, zookeeper):
    client = KafkaClient(hosts=broker[0])
    topic = client.topics[topic]

    consumer = topic.get_balanced_consumer(
        consumer_group=consumer_group,
        auto_commit_enable=True,
        zookeeper_connect=zookeeper[0],
        consumer_timeout_ms=930000
    )

    try:
        for message in consumer:
            try:
                if message is not None:
                    user_object = json.loads(message.value.decode('ascii'))
                    user_data = user_object.get('user')
                    database.write_UserData(user_data)
                else:
                    print("Message is Empty")

            except Exception as error:
                print("Error while reading messages: {}".format(error))
                break

    except Exception as error:
        print("Something went wrong while reading messages: Error -> {}".format(error))

if __name__ == '__main__':
    TwitterFollowersConsumer(topic= config.topic, broker=config.broker,
                             consumer_group=config.consumer_group, zookeeper=config.zookeeper)
