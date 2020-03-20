# Kafka-Twitter-Streaming
### Prerequisite:-
*  Install *Kafka:* https://kafka.apache.org/downloads
* *Python 3* : https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/
*  Create virtual environment in root directory of project ->  **`` python3 -m venv venv ``**
*  Change directory where  virtual environment is installed ->  **`` cd venv/bin ``**
*  Active virtual environment -> **``source  activate ``**
*  cd to the root directory of project -> **`` pip install -r requirements.txt  ``**
*  Replace twitter credentials, database credentials, kafka details and twitter username in **config.json** file
*  Create table in your database with this following schema detials: 
 **```` 
 CREATE TABLE public.twitter_userdata
(
    id character varying ,
    user_name character varying,
    secondary_name character varying,
    created_at character varying,
    description character varying,
    profile_url character varying,
    profile_img_url character varying,
    properties json,
    verified character varying,
    private character varying
) ````**

### Twitter API used:-
* GET followers/ids: https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids
* GET users/lookup: https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-lookup

### Starting zookeeper & kafka server:-
* cd to the root of kafka reposiroty, Starting zookeeper server: **``bin/zookeeper-server-start.sh config/zookeeper.properties``**
* Starting kafka server: **`` bin/kafka-server-start.sh config/server.properties ``**

### Creating kafka topic:- 
* kafka topic: **`` bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic kafka_topic``**


### Stating kafka producer and consumer:
* Start kafka producer: **`` python3 twitter_kafka_consumer.py ``**
* Start kafka consumer: **`` python3 twitter_producer_ids_lookup.py ``**