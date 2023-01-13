from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

def row_exists(cursor, id):
    cursor.execute("SELECT id FROM line WHERE id = %s",(id,))
    return cursor.fetchone() is not None

if __name__=='__main__':

    #kafka consumer
    consumer = KafkaConsumer('lineCounts',bootstrap_servers='localhost:9094', auto_offset_reset='latest',  
      value_deserializer=lambda m: json.loads(m.decode('utf-8')),)
    #database cursor
    connection = psycopg2.connect(user = "myusername",
        password = "mypassword", host = "localhost", port = "5432", database = "test")
    cursor  = connection.cursor()


    for message in consumer:
        print("before")
        #this was for measuring the runtime of the script
        print(datetime.now())
        if(row_exists(cursor, str(message.value['Line']))):
            #print(message.value['value'])
            postgres_update_query = """ UPDATE line SET value = %s WHERE id = %s"""
            record_to_update = (message.value['value'], str(message.value['Line']))
            cursor.execute(postgres_update_query, record_to_update)
            connection.commit()
        else:
            #print(message.value['value'])
            postgres_insert_query = """ INSERT INTO line (id, value) VALUES (%s,%s)"""
            record_to_insert = (str(message.value['Line']), message.value['value'])
            cursor.execute(postgres_insert_query, record_to_insert)
            connection.commit()
        print("After writing to database")
        print(datetime.now())
    print("Done with all")
    print(datetime.now())