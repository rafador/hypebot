from threading import Thread
from elasticsearch import Elasticsearch, exceptions
import settings
import logging
import json
import sys
import sentiment_functions
import emoji

class TweetTractor():
    def __init__(self):
        ''' Constructor. '''

        Thread.__init__(self)

        self.logger = logging.getLogger('thread logger')
        self.logger.setLevel(logging.INFO)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        self._connect_to_elastic()


    def _connect_to_elastic(self):
        self.logger.info("Connecting to ELASTIC...")
        self.es = Elasticsearch([settings.ELASTICSEARCH_CONNECT_STRING],)

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
            self.es.ingest.put_pipeline(id=settings.ELASTICSEARCH_TWITTER_STREAMING_PIPELINE_ID, body=uppercase_pipeline)
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
            self.es.indices.create(settings.ELASTICSEARCH_TWITTER_STREAMING_INDEX_NAME)
            self.es.indices.put_mapping(index=settings.ELASTICSEARCH_TWITTER_STREAMING_INDEX_NAME,
                                   doc_type="tweet",
                                   body=mapping)

        except:
            self.logger.warning("Index problem: " + str(sys.exc_info()[0]))
            # raise

    def _extract_emojis(self, str):
        return [c for c in str if c in emoji.UNICODE_EMOJI]


    def prepare_tweet(self, data):
        body = {}
        tweet = json.loads(data)

        self.logger.debug("Data received: " + tweet['user']['screen_name'] + " | " + tweet['timestamp_ms'])

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
        body['entities'] = tweet['extended_tweet']['entities'] if tweet.get('extended_tweet') is not None else tweet[
            'entities']
        body['user'] = tweet['user']
        body['nested_retweets_level'] = 0

        cursortweet = tweet.get('retweeted_status')

        while cursortweet is not None:
            body['nested_retweets_level'] += 1

            # augment the text
            body['text'] += ' || ' + (cursortweet['extended_tweet']['full_text']
                                      if cursortweet.get('extended_tweet') is not None
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
        # body['sentiment'] = sentiment_functions.get_tweet_sentiment(body['text'])
        # nba_sent = sentiment_functions.get_tweet_sentiment_NBA(body['text'])
        # body['sentiment_NBA_class'] = nba_sent.classification
        # body['sentiment_NBA_p_pos'] = nba_sent.p_pos
        # body['sentiment_NBA_p_neg'] = nba_sent.p_neg
        # body['sentiment_spacy_default'] = self.get_tweet_sentiment_spacy_default(body['text'])
        ascii_string = ''.join([i if ord(i) < 128 else '*' for i in body['text']])
        body['text_truncated_for_aggregation'] = (ascii_string[:240]+"... [truncated]") \
            if len(ascii_string) > 240 \
            else ascii_string
        body['tokenized_text'] = body['text'].split()
        body['text_emoji'] = self._extract_emojis(body['text'])

        body_entities_symbols = body['entities']['symbols']
        body['entities_first_symbol'] = body_entities_symbols[0]['text'].upper() \
            if len(body_entities_symbols) >= 1 \
            else None
        body['entities_second_symbol'] = body_entities_symbols[1]['text'].upper() \
            if len(body_entities_symbols) >= 2 \
            else None
        body['entities_third_symbol'] = body_entities_symbols[2]['text'].upper() \
            if len(body_entities_symbols) >= 3 \
            else None
        body['entities_first_three_symbols'] = \
            [
                body['entities_first_symbol'],body['entities_second_symbol'],body['entities_third_symbol']
            ]

        return body

    def push_prepared_tweet_to_elastic(self, prepared_tweet):
        self.es.index(index=settings.ELASTICSEARCH_TWITTER_STREAMING_INDEX_NAME,
                 doc_type="tweet",
                 id=prepared_tweet['id_str'],
                 body=prepared_tweet,
                 params={'pipeline': settings.ELASTICSEARCH_TWITTER_STREAMING_PIPELINE_ID})

    def prepare_tweet_and_push_to_elastic(self, data):
        try:
            prepared_tweet = self.prepare_tweet(data)
            try:
                self.push_prepared_tweet_to_elastic(prepared_tweet)

            except BaseException:
                # try to reconnect to elastic
                self._connect_to_elastic()
                self.push_prepared_tweet_to_elastic(prepared_tweet)

        except BaseException as e:
            self.logger.error("Error on_data: %s" % str(e))
            # raise e

