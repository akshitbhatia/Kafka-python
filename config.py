import json

with open('/home/akshit/Desktop/kafka-python/config.json', 'r') as f:
    datastore = json.load(f)

# Twitter cred
access_token= datastore.get('twitter_cred').get('access_token')
access_token_secret= datastore.get('twitter_cred').get('access_token_secret')
consumer_key= datastore.get('twitter_cred').get('consumer_key')
consumer_secret= datastore.get('twitter_cred').get('consumer_secret')
bearer_token= datastore.get('twitter_cred').get('bearer_token')

# DB Details
user=datastore.get('DataBase_cred').get('user')
host=datastore.get("DataBase_cred").get("host")
password=datastore.get('DataBase_cred').get('password')
port=datastore.get('DataBase_cred').get('port')
dbname=datastore.get('DataBase_cred').get('dbname')

# Kafka cred
topic=datastore.get('kafka_cred').get('topic')
broker=datastore.get('kafka_cred').get('broker')
zookeeper=datastore.get('kafka_cred').get('zookeeper_connect')
consumer_group=datastore.get('kafka_cred').get('consumer_group')

#Twitter username to fetch
username=datastore.get('twitter_user_to_fetch').get('user_name')


