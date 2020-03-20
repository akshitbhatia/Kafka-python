from pykafka import KafkaClient
import requests
import json
import time
import os
import config

bearer = config.bearer_token

def writeToCsv_ids(idList, path):
    with open(path, 'a') as outfile:
        for items in idList:
            outfile.write(str(items))
            outfile.write('\n')

def writeCursor_ids(idList, path):
    with open(path, 'a') as outfile:
        outfile.write(str(idList))
        outfile.write('\n')

def callUrl_ids(bt, url, ids_count):
    if (ids_count == 5):
        print("On Sleep ids: 900 seconds")
        time.sleep(900)
    headers = {'Authorization': 'Bearer {}'.format(bt)}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    return result

def callUrl_followerLookup(bt, url, batch_count):
    if(batch_count == 201):
        print("On Sleep followers lookup: 900 seconds")
        time.sleep(900)
    headers = {'Authorization': 'Bearer {}'.format(bt)}
    response = requests.get(url, headers=headers)
    result = json.loads(response.text)
    return result

def getUserData(user):
    user_data = {'id': user.get('id'),
                'user_name': user.get('screen_name'),
                'secondary_name': user.get('name'),
                'description': user.get('description'),
                'profile_url': user.get('url'),
                'profile_img_url': user.get('profile_image_url'),
                'created_at': user.get('created_at'),
                'verified': user.get('verified'),
                'private': user.get('protected')}

    properties = json.dumps({'derived': user.get('derived'),
                             'location': user.get('location'),
                            'default_profile': user.get('default_profile'),
                            'default_profile_image': user.get('default_profile_image'),
                            'withheld_in_countries': user.get('withheld_in_countries')})

    user_data['properties'] = properties

    userDataToInsert = [
    user_data['id'],
    user_data['user_name'],
    user_data['secondary_name'],
    user_data['created_at'],
    user_data['description'],
    user_data['profile_url'],
    user_data['profile_img_url'],
    user_data['properties'],
    user_data['verified'],
    user_data['private'],
    ]
    return userDataToInsert

def TwitterFollowersProducer(screen_name, broker, topic):
    # Getting first page ids
    ids_count = 0
    batch_count=0
    follower_ids=[]
    #Kafka client
    client = KafkaClient(hosts=broker[0])
    topic = client.topics[topic]
    # Started with kafka producer
    with topic.get_producer(sync=True) as producer:
        url_ids = 'https://api.twitter.com/1.1/followers/ids.json?screen_name={}'.format(screen_name)
        returnedData = callUrl_ids(bearer, url_ids, ids_count)
        followerIds = returnedData['ids']
        writeToCsv_ids(idList= followerIds, path= os.path.dirname(os.path.abspath(__file__))+'/follower_ids.csv')

        #Creating batches of 100 records & append to followerIds empty list
        for ids in range(0, len(followerIds), 100):
            batch_list=followerIds[ids:ids + 100]
            follower_ids.append(batch_list)

        # user look up Twitter API part
        for ids in follower_ids:
            batch_count += 1
            followers_batch=str(ids).strip("[]")
            final_followers_batch=followers_batch.replace("'", "")
            base_url = "https://api.twitter.com/1.1/users/lookup.json?user_id=" + final_followers_batch
            response= callUrl_followerLookup(bearer, base_url, batch_count)
            # Processing user objects
            for item in response:
                transformed_userData = getUserData(item)
                parse_json_userData = json.dumps({'user': transformed_userData})
                producer.produce(parse_json_userData.encode('ascii'))
                print("Batch Count of first Page: ", batch_count)

        if (batch_count == len(follower_ids)):
            follower_ids*=0
            batch_count = 0

        cursorValue = returnedData['next_cursor_str']
        if (cursorValue == "0"):
            print('No Record to read\nExiting task!!')
            exit()

        # Getting next page twitter ids using next page cursor value
        if (cursorValue):
            while 1:
                try:
                    ids_count = ids_count + 1
                    urlNxt_ids = 'https://api.twitter.com/1.1/followers/ids.json?screen_name={}&cursor={}'.format(screen_name, cursorValue)
                    nextResult = callUrl_ids(bearer, urlNxt_ids, ids_count)
                    if (ids_count == 5):
                        ids_count = 0
                    nextIds = nextResult.get('ids', None)
                    writeToCsv_ids(idList= nextIds, path= os.path.dirname(os.path.abspath(__file__))+'/follower_ids.csv')
                    # Getting cursor value of next page
                    cursorValue_id = nextResult.get('next_cursor_str', None)

                    # Creating batches of 100 records
                    for ids in range(0, len(nextIds), 100):
                        batch_list = nextIds[ids:ids + 100]
                        follower_ids.append(batch_list)

                    # Processing 20000 user objects (200*100=20000)
                    if len(follower_ids)==200:
                        # user look up Twitter API part, if the length of follower_ids is 200
                        for ids in follower_ids:
                            batch_count += 1
                            followers_batch = str(ids).strip("[]")
                            final_followers_batch = followers_batch.replace("'", "")
                            base_url = "https://api.twitter.com/1.1/users/lookup.json?user_id=" + final_followers_batch
                            response = callUrl_followerLookup(bearer, base_url, batch_count)
                            # Processing user objects
                            for item in response:
                                transformed_userData = getUserData(item)
                                parse_json_userData = json.dumps({'user': transformed_userData})
                                producer.produce(parse_json_userData.encode('ascii'))
                                print("Batch count of next page: ", batch_count)

                        if (batch_count == 200):
                            batch_count = 0
                            follower_ids*=0

                    elif (cursorValue_id == "0"):
                        # user look up Twitter API part
                        # if the cursor value is 0, means its the last page of getting twitter follower ids
                        for ids in follower_ids:
                            batch_count += 1
                            followers_batch = str(ids).strip("[]")
                            final_followers_batch = followers_batch.replace("'", "")
                            base_url = "https://api.twitter.com/1.1/users/lookup.json?user_id=" + final_followers_batch
                            response = callUrl_followerLookup(bearer, base_url, batch_count)
                            # Processing user objects
                            for item in response:
                                transformed_userData = getUserData(item)
                                parse_json_userData = json.dumps({'user': transformed_userData})
                                producer.produce(parse_json_userData.encode('ascii'))
                                print("Batch_count of last page: ", batch_count)
                        writeCursor_ids(idList=cursorValue_id, path=os.path.dirname(os.path.abspath(__file__))+'/Cursors_ids.csv')
                        print("Cursor Value is {}".format(cursorValue_id))
                        print('No Record to read\nExiting task!!')
                        batch_count=0
                        follower_ids*=0
                        exit()

                    # Getting cursor value of next page
                    cursorValue = nextResult.get('next_cursor_str', None)
                    writeCursor_ids(idList= cursorValue, path= os.path.dirname(os.path.abspath(__file__))+'/Cursors_ids.csv')

                except Exception as e:
                    print("Something went wrong with Twitter API ",e)
                    print("Exiting Task!!")
                    quit()

if __name__ == '__main__':
    TwitterFollowersProducer(screen_name=config.username,  broker=config.broker, topic=config.topic)

