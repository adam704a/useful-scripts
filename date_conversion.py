from pymongo import MongoClient
import dateutil
import dateutil.parser

import slack
import slack.chat

slack.api_token = 'slack_token'

c = MongoClient("mongodb://user:pass@localhost")

d = c.mj_sample
l = d.mj_sample

counter = 1

slack.chat.post_message('mjsif_pipeline',"Date conversion utility launched.",username="Date conversion utility",icon_emoji=':date:')

# Update the twitter 'created_at' field to be a python datetime object
for t in l.find():
    try:
        l.update( {'_id' : t['_id']} , {"$set" : {'created_at' : dateutil.parser.parse(t['created_at'])} }, upsert=False)
        counter += 1
    except:
        pass
    if counter % 1000000 == 0:
        slack.chat.post_message('mjsif_pipeline',str(counter) + " dates converted.",username="Date conversion utility",icon_emoji=':date:')