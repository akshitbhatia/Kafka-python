import psycopg2
import config

def write_UserData(user_data):
    try:
        connection = psycopg2.connect(user=config.user,
                                      password=config.password,
                                      host=config.host,
                                      port=config.port,
                                      database=config.dbname)
        cursor = connection.cursor()

        deleting_requested="""INSERT INTO "athlete".twitter_userdata(id,  user_name, secondary_name, created_at, description,
        profile_url, profile_img_url, properties, verified, private ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(deleting_requested, user_data)
        print('Details of user name: {} is inserted'.format(user_data[1]))
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return False

