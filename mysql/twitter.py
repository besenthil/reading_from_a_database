from tweepy import api
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import socket,time
import json

TCP_IP = "localhost"
TCP_PORT = 7077
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")


class MyStreamListener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        print (tweet["user"]["name"])
        conn.send(tweet["user"]["name"].encode("utf8")+'\n')
        return True

    def on_status(self, status):
        print(status.text)

# Variables that contains the user credentials to access Twitter API

CONSUMER_TOKEN = '5zQ57uZVSAF5JYZHPiEXwQXQr'
CONSUMER_SECRET = 'oo3qXtjwcAQEiDsLzp3gzxpas38tTBox8623wfmqOcrXZbRltZ'
ACCESS_KEY = '14638891-cXFBpf6nkHTbGA8SbDf0Xwk5TopeMjjrAKtJcBdU4'
ACCESS_SECRET = 'UnOQwSwspQmLHgta0rO0fyf8zZUQFAPzikD7u2Nt1Kkt3'

auth = OAuthHandler(CONSUMER_TOKEN,
                            CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

myStreamListener = MyStreamListener()
myStream = Stream(auth, myStreamListener)
myStream.filter(track=['cricket'])

