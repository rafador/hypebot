import os
import logging

LOGLEVEL = logging.INFO
#timers in seconds
MARKET_REFRESH_RATE=60
RETRY_RATE=5
API_TIMEOUT=2

# ELASTICSEARCH_CONNECT_STRING = "https://search-test-eipnxghgvc444it6z7hjgs5bxy.eu-west-1.es.amazonaws.com:443"
ELASTICSEARCH_CONNECT_STRING = "http://elastic:changeme@localhost:9200"
ELASTICSEARCH_TWITTER_STREAMING_INDEX_NAME = "social_profile_v4"
ELASTICSEARCH_TWITTER_STREAMING_PIPELINE_ID = "uppercase_symbols_v2"

TWITTER_AUTH_ACCESS_TOKEN = ""
TWITTER_AUTH_ACCESS_SECRET = ""
TWITTER_AUTH_API_KEY = ""
TWITTER_AUTH_API_SECRET = ""

MQTT_HOST = "m21.cloudmqtt.com"
MQTT_PORT = 13234
MQTT_USER = "xgdowwpj"
MQTT_PASS = "iWb7xsTXoaz2"

ES_INDEX_TWITTER = "social_profile_v4"
ES_INDEX_CMC = "cmc_v1"
