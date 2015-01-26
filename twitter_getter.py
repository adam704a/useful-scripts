#################################################################
## YOU MUST CHANGE THESE VALUES TO MATCH YOUR TWITTER ACCOUNT ###
########### Create an 'application' at dev.twitter.com ##########
########### And then authenticate yourself to use it ############
######... the keys you need are listed on the app's page ########
#################################################################

APP_KEY            =  'YOUR APP_KEY'           
APP_SECRET         =  'YOUR APP_SECRET'        
OAUTH_TOKEN        =  'YOUR OAUTH_TOKEN'       
OAUTH_TOKEN_SECRET =  'YOUR OAUTH_TOKEN_SECRET'

# - Make sure you copy the keys correctly, with no extra spaces
# - and key them inside of quotes, as shown on the right above.


print("\n")

print(" ===================================================================")
print(" ||        .-. __ _ .-.                                           ||")
print(" ||        |  `  / \  |                                           ||")
print(" ||        /     '.()--\                                          ||")
print(" ||       |         '._/                                          ||")
print(" ||      _| O   _   O |_    PYTHON TWITTER GETTER                 ||")
print(" ||      =\    '-'    /=    v.3.5 HACKJOB                         ||")
print(" ||        '-._____.-'      13 NOV 2014                           ||")
print(" ||        /`/\___/\`\                                            ||")
print(" ||       /\/o     o\/\                                           ||")
print(" ||      (_|         |_)                                          ||")
print(" ||        |____,____|                                            ||")
print(" ||        (____|____)                                            ||")
print(" ||                                                               ||")
print(" || This script uses the following nonstandard python libraries:  ||")
print(" || required: twython, beautifulsoup4                             ||")
print(" || optional: pymongo (and a local MongoDB server)                ||")
print(" ||                                                               ||")
print(" || See dev.twitter.com for additional API parameters             ||")
print(" || and rules and rate limits. Anger Twitter at your own risk.    ||")
print(" ||                                                               ||")
print(" || Script provided as is. Feel free to fork, edit, etc.          ||")
print(" || https://github.com/ccheaton/useful-scripts                    ||")
print(" ===================================================================")

# If you want to capture additional fields in the .csv output, make 
# certain to include both a line for the header (near line 80) and
# a line for the actual value (near line 115). They must appear in 
# the same logical order or your headers will not be aligned with
# your data.

# Import the libraries that we need

try:
    from twython import Twython
    from twython import TwythonStreamer
except:
    print("========================= ERROR =================================")
    print("You must have the Twython library installed to use this script.\n")
    print("Try typing in your terminal or Powershell: easy_install twython")
    print("=================================================================\n")
    sys.exit()

try:
    from bs4 import BeautifulSoup
except:
    print("========================= ERROR ========================================")
    print("You must have the BeautifulSoup 4 script installed to use this script.\n")
    print("Try typing in your terminal or Powershell: easy_install beautifulsoup4")
    print("======================================================================\n")
    sys.exit()

import json
import csv
import re
import time
import os,sys
from elasticsearch import Elasticsearch

# Establish some global variables
counter           = 0
header_done       = False
first_header_done = False
collection        = False
use_mongo         = False
use_elasticsearch = False
use_json_files    = False
search_terms      = ""
keep_lang         = 'en'
output_dir        = ""
tweets_per_file   = 50000

tweet_buffer_csv  = []
tweet_buffer_json = []
flush_count       = 500

################################################################
########### GATHERING USER INPUT VARIABLES #####################
################################################################

search_type  = 1
search_terms = 'Enter filter terms or "sample" to get a sample of the full "firehose"'

keep_lang    = "all"
keep_tweets  = 99999999

from pymongo import MongoClient


use_mongo      = False
use_elasticsearch = True
use_json_files = False

if use_mongo:
	uri          = 'mongodb://localhost'
	client       = MongoClient()
	database_name  = "do_tweets"
	database       = client[database_name]

	collection_name = "tweet_collection"
	collection      = database[collection_name]

filename_prefix  = ""
file_name_suffix = 1

output_format = 2


es = Elasticsearch(['http://localhost:9200'])

# Start the timer
start = time.time()

################################################################
######################### DEFINING CLASSES #####################
################################################################


# The DasTweetMaker class processes tweets for output to csv.
class DasTweetMaker():
    def clean(self,text):
        text = text.replace("\n"," ").replace("\r"," ") # Remove newline characters
        text = text.replace('"', "'") # Convert double-quotes to single quotes
        text = text.replace(','," ")  # Remove commas
        text = " ".join(text.split()) # Remove extra spaces and tabs
        return text

    def fix_source(self,a_href):
        soup = BeautifulSoup(a_href)
        src  = soup.find('a')
        if src is not None:
            return src.text.encode("utf-8","replace")
        else:
            return ""

    def create_header(self):
        global file_name_suffix

        header = []
        tweets = open(output_dir + filename_prefix + "csv_tweets_" + str(file_name_suffix) + ".csv", 'ab+')
        wr     = csv.writer(tweets, dialect='excel')

        header.append("created_at")
        header.append("tweet_id")
        header.append("lang")
        header.append("is_retweet")
        header.append("screen_name")
        header.append("tweet")
        header.append("source")
        header.append("in_reply_to_status_id")
        header.append("in_reply_to_screen_name")
        header.append("geo")
        wr.writerow(header)

        tweets.close()

    def process(self, tweet,flush=False):
        global first_header_done
        global header_done
        global file_name_suffix
        global counter
        global tweet_buffer_csv

        if first_header_done is False or (header_done is False and include_header_in_each_file is True):
            self.create_header()
            first_header_done = True
            header_done       = True

        # Create the file or append to the existing
        theOutput = []

        # Hi kids. We're writing UTF-8 which can be a pain in the ass. You shouldn't get encoding errors.
        # If you do, then for the love all all holy, don't ask me about it. Just kidding. Just don't ask
        # me about it until you have a few drinks and search StackOverflow.com for the error messages
        # And don't try to READ a csv file with Unicode/UTF-8 characters using the standard
        # python csv library.

        theOutput.append(tweet['created_at'])
        theOutput.append(tweet['id'])
        theOutput.append(tweet['lang'].encode('utf-8', 'replace'))

        if "retweeted_status" in tweet:
            theOutput.append(1)
        else:
            theOutput.append(0)

        uname = tweet['user']['screen_name'].encode('utf-8', 'replace')
        newuname = re.sub('\n','',uname)
        theOutput.append(newuname)

        twt = self.clean(tweet['text']).encode('utf-8', 'replace')
        newtwt = re.sub('\n','',twt)
        theOutput.append(newtwt)
        
        theOutput.append(self.fix_source(tweet['source']))
        theOutput.append(tweet['in_reply_to_status_id'])
        theOutput.append(tweet['in_reply_to_screen_name'])

        if tweet['geo'] is not None:
            if tweet['geo']['type'] == 'Point':
                lat = str(tweet['geo']['coordinates'][0]) + " "
                lon = str(tweet['geo']['coordinates'][1])


                theOutput.append(lat + lon)
            else:
                theOutput.append(tweet['geo'])
        else:
            theOutput.append(tweet['geo'])

        newOutput = []
        for item in theOutput:
            if item is not None:
                newOutput.append(str(item))
            else:
                newOutput.append('')

        store = ",".join(newOutput)
        tweet_buffer_csv.append(store)

        if flush:
            tweets = open(output_dir + filename_prefix + "csv_tweets_" + str(file_name_suffix) + ".csv", 'ab+')

            for t_item in tweet_buffer_csv:
                tweets.write(t_item)
                tweets.write("\n")

            tweets.close()
            tweet_buffer_csv = []



class TweetSaver():
    def __init__(self):
        self.writer = DasTweetMaker()

    def handleTweet(self,data):
        global file_name_suffix
        global output_format
        global use_mongo
        global use_json_files
        global collection
        global client
        global counter
        global tweets_per_file
        global header_done
        global tweet_buffer_json
        global tweet_buffer_csv
        global flush_count
        global keep_tweets

        # Increment the counter
        counter += 1

        flush = False

        if (counter + 1) % tweets_per_file == 0 or counter % flush_count == 0:
            flush = True

        if counter == keep_tweets:
            flush = True

        if counter % tweets_per_file == 0:
            # Increment the file name
            header_done       = False
            file_name_suffix += 1

        if output_format in (2,3):
            if use_json_files:
                # Keep the JSON files
                tweet_buffer_json.append(data)

                # TODO: Push this onto another thread
                if flush:
                    g = open(output_dir + filename_prefix + "json_tweets_" + str(file_name_suffix) + ".json", "ab+")

                    for item in tweet_buffer_json:
                        json.dump(item,g)
                        g.write("\n")

                    g.close()
                    tweet_buffer_json = []
            
            if use_elasticsearch:

                # Push the JSON into ElasticSearch
                if data['geo'] is not None:

                    # Store State Code for US
                    if data['place']['country_code'] == 'US':
                        place_full_name = data['place']['full_name']
                        data['state_code'] = place_full_name.split(',')[1].strip()

                        es.index(index=database_name, 
                            doc_type=collection_name, 
                            id=data['id'], 
                            body=data
                            )
            if use_mongo:
                
                # Push the JSON into MongoDB
                collection.insert(data)


        if output_format in (1,3):
            # Keep the CSV
            self.writer.process(data,flush)   



# This is the class that handles the streaming.
# Stream on, stream on, stream until your stream comes true...
class MyStreamer(TwythonStreamer):
    def __init__(self, APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET):
        super( MyStreamer, self ).__init__(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
        self.saver = TweetSaver()
        self.lastlog = time.time()

    def on_success(self, data):
        global counter
        global keep_lang
        global keep_tweets
        global include_header_in_each_file
        global header_done
        global tweets_per_file
        global file_name_suffix
        global output_format
        global use_mongo
        global use_json_files
        global collection
        global client
        global start

        # Are stall_warnings handled by Twython by default?
        if 'warning' in data:
            print('\nTwitter Warning:',data['warning']['code'], data['warning']['message'])

        if 'text' in data and 'warning' not in data:
            if not 'lang' in data or (keep_lang == 'all' or data['lang'] in keep_lang):

                end = time.time()
                elapsed = end - start
                since   = end - self.lastlog
                if int(elapsed) % 60 == 0 and since > 10:
                    rate = float(counter) / (elapsed/60)
                    print(counter,"tweets at a rate of",int(rate),"per minute after",int(elapsed/60),"minutes.")
                    self.lastlog = time.time()

                # The saver object handles the saving of the data
                self.saver.handleTweet(data)

                if not output_format in (1,2,3):
                    print("You entered an invalid output format.")
                    self.disconnect()

        # Disconnect if we've gathered all of the tweets requested
        if counter >= keep_tweets:
            self.disconnect()
            if use_mongo:
                client.disconnect()

    # Something went wrong... oh noes
    def on_error(self, status_code, data):
        global use_mongo
        print(status_code, data)
        print("---- END WITH ERROR ----\n\n")
        if use_mongo:
            client.disconnect()

#########################################################
########## Function needed for search API results #######
#########################################################

def processTweetsSaveAPI(results):
    saver = TweetSaver()
    init_maxid = True

    # Make sure maxid is defined
    try:
        maxid
    except:
        maxid = "x"

    for res in results['statuses']:
        if init_maxid is True:
            maxid = int(res['id_str']) - 1
            init_maxid = False
        else:
            if int(res['id_str']) < maxid:
                maxid = int(res['id_str']) - 1
        saver.handleTweet(res)
    return maxid




#########################################################
########## This is where things start to happen #########
#########################################################

if search_type == 1:
    stream = MyStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    stream.statuses.filter(track=search_terms,stall_warnings='true')


print("\nOk all done. Bye bye.\n")