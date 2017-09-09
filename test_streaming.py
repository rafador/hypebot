import sys
import os
import json
import jsonpickle
import tweepy
from textwrap import TextWrapper
from datetime import datetime
from elasticsearch import Elasticsearch
import requests
import logging
# from textblob import TextBlob


# Switching to application authentication (instead of user authentication)
auth = tweepy.OAuthHandler('2m1s1mQ55amokrW0Q0RTN2DHw', 'QAjdsUis7fnLTvNbEZmUD9RA4wdU1HNVZXerYyLgYkdVSCTU6U')
auth.set_access_token('53643197-LOTzjjxPprcWqW7djxkveth2ITHyXhSzcCqGRm8X8', 'NG0Lg6TXgS1ENtYvrhtHZ7zzkysde548MWfH1P3XhBcXa')

api = tweepy.API(auth)

# Error handling
if (not api):
    print("Problem Connecting to API")

es = Elasticsearch(['https://search-test-eipnxghgvc444it6z7hjgs5bxy.eu-west-1.es.amazonaws.com:443'])
es_indexname = "social_profile_v4"
uppercase_pipeline_id = "uppercase_symbols_v2"


root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

r = requests.get("https://api.coinmarketcap.com/v1/ticker/")
jsond = json.loads(r.content)

totrack = [('$'+e['symbol']) for e in jsond]

logging.info(totrack)

# Inherit from the StreamListener object
class MyStreamListener(tweepy.StreamListener):

    # def clean_tweet(self, tweet):
    #     '''
    #     Utility function to clean tweet text by removing links, special characters
    #     using simple regex statements.
    #     '''
    #     return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t]) | (\w +:\ / \ / \S +)", " ", tweet).split())
    #
    # def get_tweet_sentiment(self, tweet):
    #     '''
    #     Utility function to classify sentiment of passed tweet
    #     using textblob's sentiment method
    #     '''
    #     # create TextBlob object of passed tweet text
    #     analysis = TextBlob(self.clean_tweet(tweet))
    #     # set sentiment
    #     if analysis.sentiment.polarity > 0:
    #         return 'positive'
    #     elif analysis.sentiment.polarity == 0:
    #         return 'neutral'
    #     else:
    #         return 'negative'
    #
    # def get_tweet_sentiment_NBA(self, tweet):
    #     '''
    #     Utility function to classify sentiment of passed tweet
    #     using textblob's sentiment method
    #     '''
    #     # create TextBlob object of passed tweet text
    #     analysis = TextBlob(self.clean_tweet(tweet), analyzer=NaiveBayesAnalyzer())
    #     # set sentiment
    #     return analysis.sentiment
    #
    # def get_tweet_sentiment_spacy_default(self, tweet):
    #     nlp = spacy.load('en')
    #     doc = nlp(self.clean_tweet(tweet))
    #     return doc


    def on_data(self, data):
        try:
            body = {}
            tweet = json.loads(data)

            logging.info("Data received: " + tweet['user']['screen_name'] + " | " + tweet['timestamp_ms'])

            # Get Datetime
            status_object = tweet
            timestamp_ms = int(status_object['timestamp_ms'])

            # Build the Elastic object
            body['@timestamp'] = timestamp_ms
            body['text'] = tweet['extended_tweet']['full_text'] \
                if tweet.get('extended_tweet') is not None \
                else tweet['text']
            body['id_str'] = tweet['id_str']
            body['screen_name'] = tweet['user']['screen_name']
            body['user_id_str'] = tweet['user']['id_str']
            body['in_reply_to_screen_name'] = tweet['in_reply_to_screen_name']
            body['coordinates'] = tweet['coordinates']
            body['tweet_id'] = tweet['id_str']
            body['is_reply'] = 0 \
                if tweet['in_reply_to_status_id_str'] is None \
                else 1
            body['in_reply_to_status_id_str'] = tweet['in_reply_to_status_id_str']
            body['in_reply_to_user_id_str'] = tweet['in_reply_to_user_id_str']
            body['source'] = tweet['source']
            body['entities'] = tweet['extended_tweet']['entities'] if tweet.get('extended_tweet') is not None else tweet['entities']
            body['user'] = tweet['user']
            body['nested_retweets_level'] = 0

            cursortweet = tweet.get('retweeted_status')

            while cursortweet is not None:
                body['nested_retweets_level'] += 1

                # augment the text
                body['text'] += ' || '+(cursortweet['extended_tweet']['full_text'] \
                    if cursortweet.get('extended_tweet') is not None \
                    else cursortweet['text'])
                # augment the entities
                retweeted_entities = cursortweet['extended_tweet']['entities'] \
                    if cursortweet.get('extended_tweet') is not None \
                    else cursortweet['entities']

                for key in retweeted_entities:
                    if body['entities'].get(key) is None:
                        body['entities'][key] = retweeted_entities[key]
                    else:
                        body['entities'][key] += retweeted_entities[key]

                # move cursor
                cursortweet = cursortweet.get('retweeted_status')


            url_id = 0
            for url in tweet['entities']['urls']:
                url_id_str = 'urls' + str(url_id)
                body[url_id_str] = url['expanded_url']
                url_id += 1

            user_mention_id = 0
            for user_mention in tweet['entities']['user_mentions']:
                user_mention_id_str = 'user_mention' + str(user_mention_id)
                body[user_mention_id_str] = user_mention['screen_name']
                user_mention_id += 1

            if hasattr(tweet, 'extended_entities'):
                media_id = 0
                for media in tweet['entities']['media']:
                    media_id_str = 'media' + str(media_id)
                    body[media_id_str] = media['media_url_https']
                    media_id += 1

            # saving sentiment of tweet
            # body['cleaned_text'] = self.clean_tweet(body['text'])
            # body['sentiment'] = self.get_tweet_sentiment(body['text'])
            # body['sentiment_NBA_class'] = self.get_tweet_sentiment_NBA(body['text']).classification
            # body['sentiment_NBA_p_pos'] = self.get_tweet_sentiment_NBA(body['text']).p_pos
            # body['sentiment_NBA_p_neg'] = self.get_tweet_sentiment_NBA(body['text']).p_neg
            # body['sentiment_spacy_default'] = self.get_tweet_sentiment_spacy_default(body['text'])

            es.index(index=es_indexname, doc_type="tweet", body=body, params={'pipeline':uppercase_pipeline_id})


        # Error handling
        except BaseException as e:
            logging.error("Error on_data: %s" % str(e))
            logging.error("Extended tweet: %s" % tweet.get('extended_tweet'))
            raise e

        return True

    # Error handling
    def on_status(self, status):
        logging.error(status)
        return True

    # Error handling
    def on_error(self, status):
        logging.error(status)
        return True

    # Timeout handling
    def on_timeout(self):
        logging.error("Timeout")
        return True



try:
    uppercase_pipeline = {
        "description": "Uppercases all symbol entities",
        "processors": [
        {
            "foreach":
            {
                "field": "entities.symbols",
                "processor":
                {
                    "uppercase": {
                        "field": "_ingest._value.text"
                    }
                }
            }
        }
        ]
    }
    # try:
    #     es.ingest.delete_pipeline(id=uppercase_pipeline_id)
    # except:
    #     pass
    es.ingest.put_pipeline(id=uppercase_pipeline_id, body=uppercase_pipeline)
except:
    logging.warning("Pipeline exists")
    # raise

try:
    mapping = {
        "tweet": {
            "properties": {
                "@timestamp": {"type": "date"}
            }
        }
    }

    # try:
    #     es.indices.delete(es_indexname)
    # except:
    #     pass
    es.indices.create(es_indexname)
    es.indices.put_mapping(index=es_indexname, doc_type="tweet", body=mapping)

except:
    logging.warning("Index problem: " + str(sys.exc_info()[0]))
    # raise


#Create a stream object
twitter_stream = tweepy.Stream(auth, MyStreamListener())
try:
    twitter_stream.filter(track=totrack[0:300], stall_warnings=True)
finally:
    twitter_stream.disconnect()
