#!/usr/bin/env python
# coding: utf-8
from tweepy import Stream 
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from pymongo import MongoClient
import config
import json
import time



# consumer key, consumer secret, access token, access secret from config.py file.
consumer_key=config.consumer_key
consumer_secret=config.consumer_secret
access_token=config.access_token
access_secret=config.access_secret

# Twitter collection tweets keywords from config.py file.
keywords=config.twitter_keywords

# Configure MongoDB connection parameters from config.py file.
connect = MongoClient(config.mongo_server, config.mongo_port,username=config.mongo_password,password=config.mongo_password)
database = connect[config.mongo_database]
collection = database[config.mongo_collection]


class listener(StreamListener):

    def on_data(self, data):
        try:
            
            # Convert from JSON format to Python dict-like structure
            tweet = json.loads(json_data)
            
            # insert the information in the spesified database collection
            collection.insert(data)
            print(tweet)

        except KeyError as e:
            print("KeyError :",str(e))
        return(True)

    def on_error(self, status):
        print("on_error: ",status)


while True:
    try:
        # Autehticate to Twitter API
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)

        # Configure StreamListener to parameters provided (tweet_mode: extended allows full_text retrieve from streaming API)
        twitterStream = Stream(auth, listener(),tweet_mode='extended')

        # Configure filter to StreamListener
        twitterStream.filter(languages=["fi"],track=keywords)
    except Exception as e:
        print("Exception :",str(e))
        time.sleep(5)




