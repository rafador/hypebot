from elasticsearch import Elasticsearch
from time import sleep
import logging
import requests
import json
import sys
from datetime import datetime

import settings

def main():
    logging.basicConfig(format='%(levelname)s:%(asctime)s %(message)s',level=settings.LOGLEVEL)
    es = Elasticsearch(settings.ELASTICSEARCH_CONNECT_STRING)
    es_indexname = "cmc_v1"


    logging.info('Market Refresh Rate: ' + str(settings.MARKET_REFRESH_RATE) + ' seconds.')

    logging.info('Application Started.')

    try:
        mapping = {
            "ticker": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "last_updated": {"type": "date"},
                    "rank": {"type": "double"},
                    "price_usd": {"type": "double"},
                    "price_btc": {"type": "double"},
                    "24h_volume_usd": {"type": "double"},
                    "market_cap_usd": {"type": "double"},
                    "available_supply": {"type": "double"},
                    "total_supply": {"type": "double"},
                    "percent_change_1h": {"type": "double"},
                    "percent_change_24h": {"type": "double"},
                    "percent_change_7d": {"type": "double"},
                }
            }
        }

        # try:
        #     es.indices.delete(es_indexname)
        # except:
        #     pass
        es.indices.create(es_indexname)
        es.indices.put_mapping(index=es_indexname, doc_type="ticker", body=mapping)

    except:
        logging.warning("Index problem: " + str(sys.exc_info()[0]))
        # raise

    #Record Ticks
    while True:
        try:
            r = requests.get("https://api.coinmarketcap.com/v1/ticker/")
            jsond = json.loads(r.content)

            for e in jsond:
                e['@timestamp'] = datetime.utcnow()
                es.index(index=es_indexname, doc_type="ticker", body=e)

            logging.info("Prises imported")

            sleep(settings.MARKET_REFRESH_RATE)

        except Exception as e:
            logging.error(e)
            sleep(settings.RETRY_RATE)

if __name__ == '__main__':
    main()
