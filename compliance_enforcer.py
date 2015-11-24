#This script takes all tweets in a collection and updates them with the latest version of 
#the tweet object from Twitter.  If the tweet no longer exists, the tweet is removed from 
#the collection.  This script can check ~1.7 million tweets per day 

#PAR 110915

import sys
import time
import json
from pymongo import MongoClient
from twython import Twython 
import requests
import slack
import slack.chat


SLACK_KEY = ''
slack.api_token = SLACK_KEY

#-------------------User Defined Start-------------------------------------------------#
def post_to_slack(msg):
    slack.chat.post_message('mjsif_pipeline', msg, username="twitter_getter.py", icon_emoji=":taco:")
                    



#Twitter Keys
APP_KEY            =  ''           
APP_SECRET         =  ''        
OAUTH_TOKEN        =  ''       
OAUTH_TOKEN_SECRET =  ''

#Mongo URI
uri = ""

#Mongo DB
db = ''

#Mongo Collection
col = ''


#-------------------User Defined End---------------------------------------------------#

post_to_slack("Starting Compliance Enforcer")


#Setting up Mongo Connection
c   = MongoClient(uri)
d   = c[db]
collection = d[col]

#make sure tweet_id is indexed on this collection
collection.ensure_index("id")


#Get list of tweet_ids
fields = {'_id':0, 'id' :1}
ids_to_check = list(collection.find({}, fields))
ids_to_check_list=[]
for ids in ids_to_check:
    ids_to_check_list.append(ids['id'])
del ids_to_check

starting_tweet_num=len(ids_to_check_list)

post_to_slack(str(starting_tweet_num) + " tweets to check")

while len(ids_to_check_list) > 0: 
    try:
        # Establish the Twython object that will do the work
        twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

        #Get API Status
        apistatus  = twitter.get_application_rate_limit_status()
        rate_limit = apistatus['resources']['statuses']['/statuses/lookup']['remaining']
        reset_time = apistatus['resources']['statuses']['/statuses/lookup']['reset']

        if rate_limit == 0:
            our_time = int(time.time())
            seconds_to_wait = abs(reset_time - our_time + 100)
            time.sleep(seconds_to_wait)
            apistatus  = twitter.get_application_rate_limit_status()
            rate_limit = apistatus['resources']['statuses']['/statuses/lookup']['remaining']

        #Start the cycle
        while len(ids_to_check_list) > 0:
            
            while len(ids_to_check_list) > 100:
                
                while rate_limit > 0:
                    id_batch = ids_to_check_list[0:100]
                    results = twitter.lookup_status(id=id_batch, include_entities=True, map=True) 
                    for tweet in results['id']:
                        tweet_id = int(tweet)
                        tweet_object=results['id'][tweet]
                        if tweet_object == None:
                            collection.remove(spec_or_id={'id': tweet_id})
                        else:
                            collection.update({'id': tweet_id}, tweet_object)

                    #remove ids we just checked
                    ids_to_check_list=ids_to_check_list[100:]

                    #update rate_limit
                    rate_limit = rate_limit-1 


                #Get API Status
                apistatus  = twitter.get_application_rate_limit_status()
                rate_limit = apistatus['resources']['statuses']['/statuses/lookup']['remaining']
                reset_time = apistatus['resources']['statuses']['/statuses/lookup']['reset']

                while rate_limit == 0:
                    our_time = int(time.time())
                    seconds_to_wait = abs(reset_time - our_time + 120)
                    time.sleep(seconds_to_wait)
                    apistatus  = twitter.get_application_rate_limit_status()
                    rate_limit = apistatus['resources']['statuses']['/statuses/lookup']['remaining']

                post_to_slack(str(len(ids_to_check_list)) + " tweets to check")

            #Send the last batch through
            id_batch = ids_to_check_list[0:100]
            results = twitter.lookup_status(id=id_batch, include_entities=True, map=True) 
            for tweet in results['id']:
                tweet_id = int(tweet)
                tweet_object=results['id'][tweet]
                if tweet_object == None:
                    collection.remove(spec_or_id={'id': tweet_id})
                else:
                    collection.update({'id': tweet_id}, tweet_object)

        post_to_slack("Complete! " + str(starting_tweet_num - collection.count()) + " tweets deleted.")

    except Exception as e:
        print(e)
        post_to_slack("Error.  Waiting 16 minutes then retrying")
        time.sleep(960)





