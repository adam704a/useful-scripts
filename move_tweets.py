from pymongo import MongoClient
import dateutil
import dateutil.parser
import datetime 

import slack
import slack.chat

slack.api_token = 'SLACK_KEY'


c1 = MongoClient("SOURCE_MONGO_URL")
d1 = c1.mj_tweets
l1 = d1.mj_sample


c2 = MongoClient("DESTINATION_MONGO_URL")
d2 = c2.mj_sample
l2 = d2.mj_sample

start_time = datetime.datetime(2015, 6, 2)
end_time = datetime.datetime(2015, 6, 24)

counter = 1

slack.chat.post_message('mjsif_pipeline',"Tweet Mover launched.",username="Tweet Mover Utility",icon_emoji=':taxi:')

for t in l1.find( {"created_at" : { "$lte" : end_time, "$gte": start_time}},{'_id': False} ):
    try:
        l2.insert(t)
        counter += 1
    except:
        pass
    if counter % 10000 == 0:
        slack.chat.post_message('mjsif_pipeline',str(counter) + " tweets moved",username="Date Mover Utility",icon_emoji=':taxi:')