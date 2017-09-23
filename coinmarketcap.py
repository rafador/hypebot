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

                    "percent_change_1h_to_btc": {"type": "double"},
                    "percent_change_24h_to_btc": {"type": "double"},
                    "percent_change_7d_to_btc": {"type": "double"},

                    "percent_change_1h_to_gmc": {"type": "double"},
                    "percent_change_24h_to_gmc": {"type": "double"},
                    "percent_change_7d_to_gmc": {"type": "double"},

                    "price_usd_derivative": {"type": "double"},
                    "price_btc_derivative": {"type": "double"},

                    "percent_change_1h_derivative": {"type": "double"},
                    "percent_change_24h_derivative": {"type": "double"},
                    "percent_change_7d_derivative": {"type": "double"},

                    "percent_change_1h_to_btc_derivative": {"type": "double"},
                    "percent_change_24h_to_btc_derivative": {"type": "double"},
                    "percent_change_7d_to_btc_derivative": {"type": "double"},

                    "percent_change_1h_to_gmc_derivative": {"type": "double"},
                    "percent_change_24h_to_gmc_derivative": {"type": "double"},
                    "percent_change_7d_to_gmc_derivative": {"type": "double"},

                    "percent_change_1h_magnitude": {"type": "double"},
                    "percent_change_24h_magnitude": {"type": "double"},
                    "percent_change_7d_magnitude": {"type": "double"},

                    "percent_change_1h_to_btc_magnitude": {"type": "double"},
                    "percent_change_24h_to_btc_magnitude": {"type": "double"},
                    "percent_change_7d_to_btc_magnitude": {"type": "double"},

                    "percent_change_1h_to_gmc_magnitude": {"type": "double"},
                    "percent_change_24h_to_gmc_magnitude": {"type": "double"},
                    "percent_change_7d_to_gmc_magnitude": {"type": "double"}
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
            # get ticker for all currencies
            r = requests.get("https://api.coinmarketcap.com/v1/ticker/")
            jsond = json.loads(r.content)

            jsondBTC = jsond[0]

            # calculate the global market cap change
            gmc_before_1h = 0
            gmc_before_24h = 0
            gmc_before_7d = 0
            gmc_now = 0

            for e in jsond:
                # now-before / before = now/before - 1 = now_diff
                # => now / before = now_diff+1
                # => before = now / now_diff+1
                now_diff_1h  = float(e['percent_change_1h']) / 100 if e['percent_change_1h'] is not None else 0
                now_diff_24h = float(e['percent_change_24h']) / 100 if e['percent_change_24h'] is not None else 0
                now_diff_7d  = float(e['percent_change_7d']) / 100 if e['percent_change_7d'] is not None else 0
                now = float(e['market_cap_usd']) if e['market_cap_usd'] is not None else 0

                before_1h  = now  / (now_diff_1h + 1)
                before_24h = now  / (now_diff_24h + 1)
                before_7d  = now  / (now_diff_7d + 1)

                gmc_now += now
                gmc_before_1h += before_1h
                gmc_before_24h += before_24h
                gmc_before_7d += before_7d

            # base of division is the "before" mc, because from 1 to 2 is 100% increase.
            # Otherwise from 1 to 2 will be 50% increase, 1 to 3 will be 66% increase, etc.. ( 3 - 1 / 3 = 0.66 )
            gmc_percent_change_1h   = ( gmc_now / gmc_before_1h - 1 ) * 100
            gmc_percent_change_24h  = ( gmc_now / gmc_before_24h - 1 ) * 100
            gmc_percent_change_7d   = ( gmc_now / gmc_before_7d - 1 ) * 100

            for e in jsond:
                e['@timestamp'] = datetime.utcnow()
                e['last_updated'] = int(e['last_updated']) * 1000

                e["rank"]                = float(e['rank']) \
                    if e['rank'] is not None else 0.0
                e["price_usd"]           = float(e['price_usd']) \
                    if e['price_usd'] is not None else 0.0
                e["price_btc"]           = float(e['price_btc']) \
                    if e['price_btc'] is not None else 0.0
                e["24h_volume_usd"]      = float(e['24h_volume_usd']) \
                    if e['24h_volume_usd'] is not None else 0.0
                e["market_cap_usd"]      = float(e['market_cap_usd']) \
                    if e['market_cap_usd'] is not None else 0.0
                e["available_supply"]    = float(e['available_supply']) \
                    if e['available_supply'] is not None else 0.0
                e["total_supply"]        = float(e['total_supply']) \
                    if e['total_supply'] is not None else 0.0

                e['percent_change_1h'] = float(e['percent_change_1h']) \
                    if e['percent_change_1h'] is not None else 0.0
                e['percent_change_24h'] = float(e['percent_change_24h']) \
                    if e['percent_change_24h'] is not None else 0.0
                e['percent_change_7d'] = float(e['percent_change_7d']) \
                    if e['percent_change_7d'] is not None else 0.0


                # augment with data
                e["percent_change_1h_magnitude"] = abs(float(e['percent_change_1h'])) \
                    if e['percent_change_1h'] is not None else 0.0
                e["percent_change_24h_magnitude"] = abs(float(e['percent_change_24h'])) \
                    if e['percent_change_24h'] is not None else 0.0
                e["percent_change_7d_magnitude"] = abs(float(e['percent_change_7d'])) \
                    if e['percent_change_7d'] is not None else 0.0

                e["percent_change_1h_to_btc"] = \
                    (float(e['percent_change_1h']) - float(jsondBTC['percent_change_1h'])) \
                        if e['percent_change_1h'] is not None else 0.0
                e["percent_change_24h_to_btc"] = \
                    (float(e['percent_change_24h']) - float(jsondBTC['percent_change_24h'])) \
                        if e['percent_change_24h'] is not None else 0.0
                e["percent_change_7d_to_btc"] = \
                    (float(e['percent_change_7d']) - float(jsondBTC['percent_change_7d'])) \
                        if e['percent_change_7d'] is not None else 0.0

                e["percent_change_1h_to_btc_magnitude"] = abs(float(e['percent_change_1h_to_btc']))
                e["percent_change_24h_to_btc_magnitude"] = abs(float(e['percent_change_24h_to_btc']))
                e["percent_change_7d_to_btc_magnitude"] = abs(float(e['percent_change_7d_to_btc']))

                e["percent_change_1h_to_gmc"] = \
                    (float(e['percent_change_1h']) - gmc_percent_change_1h) \
                        if e['percent_change_1h'] is not None else 0.0
                e["percent_change_24h_to_gmc"] = \
                    (float(e['percent_change_24h']) - gmc_percent_change_24h) \
                        if e['percent_change_24h'] is not None else 0.0
                e["percent_change_7d_to_gmc"] = \
                    (float(e['percent_change_7d']) - gmc_percent_change_7d) \
                        if e['percent_change_7d'] is not None else 0.0

                e["percent_change_1h_to_gmc_magnitude"] = abs(float(e['percent_change_1h_to_gmc']))
                e["percent_change_24h_to_gmc_magnitude"] = abs(float(e['percent_change_24h_to_gmc']))
                e["percent_change_7d_to_gmc_magnitude"] = abs(float(e['percent_change_7d_to_gmc']))

                e["percent_change_1h_to_gmc_derivative"] = \
                    calc_derivative(e['id'], "percent_change_1h_to_gmc", e['@timestamp'], e["percent_change_1h_to_gmc"], e['last_updated'])
                e["percent_change_24h_to_gmc_derivative"] = \
                    calc_derivative(e['id'], "percent_change_24h_to_gmc", e['@timestamp'], e["percent_change_24h_to_gmc"], e['last_updated'])
                e["percent_change_7d_to_gmc_derivative"] = \
                    calc_derivative(e['id'], "percent_change_7d_to_gmc", e['@timestamp'], e["percent_change_7d_to_gmc"], e['last_updated'])

                e["percent_change_1h_to_btc_derivative"] = \
                    calc_derivative(e['id'], "percent_change_1h_to_btc", e['@timestamp'],
                                    e["percent_change_1h_to_btc"], e['last_updated'])
                e["percent_change_24h_to_btc_derivative"] = \
                    calc_derivative(e['id'], "percent_change_24h_to_btc", e['@timestamp'],
                                    e["percent_change_24h_to_btc"], e['last_updated'])
                e["percent_change_7d_to_btc_derivative"] = \
                    calc_derivative(e['id'], "percent_change_7d_to_btc", e['@timestamp'],
                                    e["percent_change_7d_to_btc"], e['last_updated'])

                e["percent_change_1h_derivative"] = \
                    calc_derivative(e['id'], "percent_change_1h", e['@timestamp'],
                                    e["percent_change_1h"], e['last_updated'])
                e["percent_change_24h_derivative"] = \
                    calc_derivative(e['id'], "percent_change_24h", e['@timestamp'],
                                    e["percent_change_24h"], e['last_updated'])
                e["percent_change_7d_derivative"] = \
                    calc_derivative(e['id'], "percent_change_7d", e['@timestamp'],
                                    e["percent_change_7d"], e['last_updated'])

                propname = "price_usd"
                e[propname+"_derivative"] = \
                    calc_derivative(e['id'], propname, e['@timestamp'],
                                    e[propname], e['last_updated'])

                propname = "price_btc"
                e[propname + "_derivative"] = \
                    calc_derivative(e['id'], propname, e['@timestamp'],
                                    e[propname], e['last_updated'])

                propname = "24h_volume_usd"
                e[propname + "_derivative"] = \
                    calc_derivative(e['id'], propname, e['@timestamp'],
                                    e[propname], e['last_updated'])

                es.index(index=es_indexname, doc_type="ticker", body=e)

            logging.info("Prises imported")

            sleep(settings.MARKET_REFRESH_RATE)

        except Exception as e:
            logging.error(e)
            sleep(settings.RETRY_RATE)

buffer_for_derivative_calc = {}
def calc_derivative(symbol, id, x, y, last_updated):

    if  buffer_for_derivative_calc.get(symbol) is None:
        buffer_for_derivative_calc[symbol] = {}
        buffer_for_derivative_calc[symbol][id] = {}
        buffer_for_derivative_calc[symbol][id]['y'] = y
        buffer_for_derivative_calc[symbol][id]['x'] = x
        buffer_for_derivative_calc[symbol][id]['last_updated'] = '0'
        return None

    if  buffer_for_derivative_calc[symbol].get(id) is None:
        buffer_for_derivative_calc[symbol][id] = {}
        buffer_for_derivative_calc[symbol][id]['y'] = y
        buffer_for_derivative_calc[symbol][id]['x'] = x
        buffer_for_derivative_calc[symbol][id]['last_updated'] = '0'
        return None

    if  buffer_for_derivative_calc[symbol][id]['last_updated'] == last_updated and \
        buffer_for_derivative_calc[symbol][id]['y'] == y:
        return None

    prev_y = buffer_for_derivative_calc[symbol][id]['y']
    prev_x = buffer_for_derivative_calc[symbol][id]['x']

    buffer_for_derivative_calc[symbol][id]['x'] = x
    buffer_for_derivative_calc[symbol][id]['y'] = y
    buffer_for_derivative_calc[symbol][id]['last_updated'] = last_updated

    der = (y - prev_y) # / (x - prev_x).total_seconds()

    # buffer_for_derivative_calc[symbol][id]['der'] = der

    # coinmarketcap refreshes every 5 mins. if refresh rate is 1 min, multiply by 5 to account for aprox. four 0s which come in between
    return float(der) # * 5


if __name__ == '__main__':
    main()
