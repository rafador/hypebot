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

r = requests.get("https://api.coinmarketcap.com/v1/ticker/")
jsond = json.loads(r.content)

totrack = " OR ".join(('$'+e['symbol']) for e in jsond)

print(totrack)