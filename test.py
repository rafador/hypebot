import sys
import os
import jsonpickle
import tweepy
from textwrap import TextWrapper
from datetime import datetime
from elasticsearch import Elasticsearch

# Switching to application authentication (instead of user authentication)
auth = tweepy.AppAuthHandler('2m1s1mQ55amokrW0Q0RTN2DHw', 'QAjdsUis7fnLTvNbEZmUD9RA4wdU1HNVZXerYyLgYkdVSCTU6U')
# auth.set_access_token('53643197-LOTzjjxPprcWqW7djxkveth2ITHyXhSzcCqGRm8X8', 'NG0Lg6TXgS1ENtYvrhtHZ7zzkysde548MWfH1P3XhBcXa')

api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

# Error handling
if (not api):
    print("Problem Connecting to API")

#Getting Geo ID for USA
# places = api.geo_search(query="USA", granularity="country")
#Copy USA id
#place_id = places[0].id
#print('USA id is: ',place_id)

# public_tweets = api.home_timeline()
# for tweet in public_tweets:
#     print('===')
#     print(tweet.user.screen_name)
#     print('---')
#     print(tweet.text)


ES_HOST = {'host': ip, 'port': port}
es = Elasticsearch(hosts=[ES_HOST])


searchQuery = '#crypto'
#Maximum number of tweets we want to collect
maxTweets = 1000000
#The twitter Search API allows up to 100 tweets per query
tweetsPerQry = 500

tweetCount = 0

# Open a text file to save the tweets to
with open('CRYPTO.json', 'w') as f:
    # Tell the Cursor method that we want to use the Search API (api.search)
    # Also tell Cursor our query, and the maximum number of tweets to return
    for tweet in tweepy.Cursor(api.search, q=searchQuery).items(tweetsPerQry):
        # Write the JSON format to the text file, and add one to the number of tweets we've collected
        print("Got {0} tweets".format(tweetCount))
        f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')

        es.index(index='twitter', doc_type="trial", id=random.random(), body=tweet._json)

        es.create(index='twitter.search',
                  doc_type='',
                  body=tweet._json)
        tweetCount += 1

    # Display how many tweets we have collected
    print("Downloaded {0} tweets".format(tweetCount))