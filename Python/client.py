from __future__ import print_function
import grpc
import requests
import threading
import io
import pubsub_api_pb2 as pb2
import pubsub_api_pb2_grpc as pb2_grpc
import avro.schema
import avro.io
import time
import certifi
import json
import sqlite3




def init_db():
    connection_obj = sqlite3.connect('pubsub.db')
    cursor_obj = connection_obj.cursor()

    cursor_obj.execute("DROP TABLE IF EXISTS AUTH")
    table = """ CREATE TABLE AUTH (
                Token VARCHAR(255) NOT NULL,
                Url VARCHAR(255) NOT NULL
                ); """
    
    cursor_obj.execute(table)
    
    print("Table is Ready")
    
    # Close the connection
    connection_obj.close()


print ('>>>>>EXecution Starts')



def call_api():
    semaphore = threading.Semaphore(1)
    latest_replay_id = None
    with open(certifi.where(), 'rb') as f:
        creds = grpc.ssl_channel_credentials(f.read())
    with grpc.secure_channel('api.pubsub.salesforce.com:7443', creds) as channel:

        print('>>>>>>>AUTH Start>>>>>>>>')
        authurl = 'https://login.salesforce.com/services/oauth2/token'
        payload={
            'grant_type': 'password',
            'client_id': '3MVG9ZL0ppGP5UrAP59A8.dNkSsWx54hRgtkftFHZh1bxEMSGF6kwnRNA8VheLBe2RHROd01KucH2QHHt5ggh',
            'client_secret': '22883FEE07FDBA0FD71F317F8A4821155253D7853C280B4302EA229A30B1DDEF',
            'username': 'ashutoshexams@gmail.com',
            'password': 'ashutosh9351aozWQvXaav1CPDQaHHgtU7pQy',
            'organizationId': '00D28000000dVa8EAE'
        }
        '''payload = {
            'grant_type': 'password',
            'client_id': clientId,
            'client_secret': clientSecret,
            'username': username,
            'password': password,
        }'''
        res = requests.post(authurl, 
            headers={"Content-Type":"application/x-www-form-urlencoded"},
            data=payload)
        token = ''
        url= ''
        if res.status_code == 200:
            response = res.json()
        
            access_token = response['access_token']
            instance_url = response['instance_url']
            access_token = access_token.encode(encoding="ascii",errors="ignore").decode('utf-8')
            instance_url = instance_url.encode(encoding="ascii",errors="ignore").decode('utf-8')
            print(access_token)
            print(instance_url)
            print('>>>>>>>AUTH Ends>>>>>>>>')
            conn = sqlite3.connect('pubsub.db')
            print ('DB IS OPen')

            conn.execute("""INSERT INTO AUTH (Token,Url) \
                VALUES (?,?)""",(access_token,instance_url));
            conn.commit()


            print('>>>>>>>DB INSERTION Ends>>>>>>>>')

            cursor = conn.execute("SELECT  Token, Url from Auth")
            for row in cursor:
                print ("Token = ", row[0])
                print ("Url = ", row[1] +"/n")
                token = row[0]
                url = row[1]
                print ("Operation done successfully");
            conn.close()

        print("Token is",token);
        print("url is",url);
        authmetadata = (('accesstoken', token),('instanceurl', url),('tenantid', '00D28000000dVa8EAE'))


        stub = pb2_grpc.PubSubStub(channel)
        def fetchReqStream(topic):
            while True:
                semaphore.acquire()
                yield pb2.FetchRequest(
                    topic_name = topic,
                    replay_preset = pb2.ReplayPreset.LATEST,
                    num_requested = 1)

        def decode(schema, payload):
            schema = avro.schema.parse(schema)
            buf = io.BytesIO(payload)
            decoder = avro.io.BinaryDecoder(buf)
            reader = avro.io.DatumReader(schema)
            ret = reader.read(decoder)
            return ret

        mysubtopic = "/data/AccountChangeEvent"
        print('Subscribing to ' + mysubtopic)
        substream = stub.Subscribe(fetchReqStream(mysubtopic),
                metadata=authmetadata)
        for event in substream:
            if event.events:
                semaphore.release()
                print("Number of events received: ", len(event.events))
                payloadbytes = event.events[0].event.payload
                schemaid = event.events[0].event.schema_id
                schema = stub.GetSchema(
                        pb2.SchemaRequest(schema_id=schemaid),
                        metadata=authmetadata).schema_json
                decoded = decode(schema, payloadbytes)
                print("Got an event!", json.dumps(decoded))
            else:
                print("[", time.strftime('%b %d, %Y %l:%M%p %Z'),
                "] The subscription is active.")
                latest_replay_id = event.latest_replay_id

init_db();
call_api();
    

