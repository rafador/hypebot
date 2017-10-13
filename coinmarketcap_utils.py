from datetime import datetime
from coinmarketcap_aggregations import *
import logging
import settings
from elasticsearch import Elasticsearch

es = Elasticsearch(settings.ELASTICSEARCH_CONNECT_STRING)
def query_es_for_cmc_last_value(coinID, propname):
    res = es.search(index=settings.ES_INDEX_CMC,
                    body={
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "query_string": {
                                "query": f"id.keyword: {coinID}",
                                "analyze_wildcard": "false"
                              }
                            }
                          ]
                        }
                      },
                      "size": 1,
                      "sort": [
                        {
                          "@timestamp": {
                            "order": "desc"
                          }
                        }
                      ]
                    })
    return res[0][propname]


def scale_value_to_unit_range(val,
                         top,
                         bottom):
    # level_zero =  0
    level_top  = top - bottom
    level_val  = val - bottom
    #scale to between 0 and 1
    factor = 1 / level_top \
        if level_top != 0 else 0

    return level_val * factor


def make_float(input):
    return float(input) \
        if input is not None else 0.0

def transform_for_elastic(e,
                          btc_percent_change_1h,
                          btc_percent_change_24h,
                          btc_percent_change_7d,
                          gmc_percent_change_1h,
                          gmc_percent_change_24h,
                          gmc_percent_change_7d):

    # standard data
    e["rank"]           = make_float(e['rank'])
    e["price_usd"]      = make_float(e['price_usd'])
    e["price_btc"]      = make_float(e['price_btc'])
    e["24h_volume_usd"] = make_float(e['24h_volume_usd'])
    e["market_cap_usd"] = make_float(e['market_cap_usd'])
    e["available_supply"] = make_float(e['available_supply'])
    e["total_supply"]   = make_float(e['total_supply'])
    e['percent_change_1h'] = make_float(e['percent_change_1h'])
    e['percent_change_24h'] = make_float(e['percent_change_24h'])
    e['percent_change_7d'] = make_float(e['percent_change_7d'])

    # calculated data
    # market cap in btc
    e["market_cap_btc"] = e['price_btc'] * e['available_supply'] \
        if e['price_btc'] is not None and \
           e['available_supply'] is not None else None

    # volume rank
    e['volume_percent_of_market_cap'] = ( e['24h_volume_usd'] / e['market_cap_usd'] ) * 100 \
        if e['market_cap_usd'] is not None and \
           e['market_cap_usd'] > 0 else None

    # changes against btc and gmt
    e["percent_change_1h_to_btc"] = (e['percent_change_1h'] - btc_percent_change_1h)
    e["percent_change_24h_to_btc"] = (e['percent_change_24h'] - btc_percent_change_24h)
    e["percent_change_7d_to_btc"] = (e['percent_change_7d'] - btc_percent_change_7d)
    e["percent_change_1h_to_gmc"] = (e['percent_change_1h'] - gmc_percent_change_1h)
    e["percent_change_24h_to_gmc"] = (e['percent_change_24h'] - gmc_percent_change_24h)
    e["percent_change_7d_to_gmc"] = (e['percent_change_7d'] - gmc_percent_change_7d)
    # magnitudes of the changes
    e["percent_change_1h_magnitude"] = abs(e['percent_change_1h'])
    e["percent_change_24h_magnitude"] = abs(e['percent_change_24h'])
    e["percent_change_7d_magnitude"] = abs(e['percent_change_7d'])
    e["percent_change_1h_to_btc_magnitude"] = abs(e['percent_change_1h_to_btc'])
    e["percent_change_24h_to_btc_magnitude"] = abs(e['percent_change_24h_to_btc'])
    e["percent_change_7d_to_btc_magnitude"] = abs(e['percent_change_7d_to_btc'])
    e["percent_change_1h_to_gmc_magnitude"] = abs(e['percent_change_1h_to_gmc'])
    e["percent_change_24h_to_gmc_magnitude"] = abs(e['percent_change_24h_to_gmc'])
    e["percent_change_7d_to_gmc_magnitude"] = abs(e['percent_change_7d_to_gmc'])
    # derivatives of the changes
    e = add_derivative_for_property(e, "percent_change_1h_to_gmc")
    e = add_derivative_for_property(e, "percent_change_24h_to_gmc")
    e = add_derivative_for_property(e, "percent_change_7d_to_gmc")

    e = add_derivative_for_property(e, "percent_change_1h_to_btc")
    e = add_derivative_for_property(e, "percent_change_24h_to_btc")
    e = add_derivative_for_property(e, "percent_change_7d_to_btc")

    e = add_derivative_for_property(e, "percent_change_1h")
    e = add_derivative_for_property(e, "percent_change_24h")
    e = add_derivative_for_property(e, "percent_change_7d")

    # derivatives of other stuff
    e = add_derivative_for_property(e, "price_usd")
    e = add_derivative_for_property(e, "price_btc")
    e = add_derivative_for_property(e, "24h_volume_usd")
    e = add_derivative_for_property(e, "market_cap_usd")
    e = add_derivative_for_property(e, "market_cap_btc")

    return e



# TODO: pickle
buffer_for_derivative_calc = {}

def init_buffer_for_coinID_and_propname(coinID, propname, y):
    # TODO: try query from elastic
    buffer_for_derivative_calc[coinID] = {}
    init_buffer_for_propname(coinID, propname, y)

def init_buffer_for_propname(coinID, propname, y):
    # TODO: try query from elastic
    smoothness = 1000
    buffer_for_derivative_calc[coinID][propname] = {}
    buffer_for_derivative_calc[coinID][propname]['y'] = collections.deque(smoothness * [y], smoothness)
    buffer_for_derivative_calc[coinID][propname]['last_updated'] = '0'

def add_derivative_for_property(_in, propname):
    coinID          = _in['id']
    y               = _in[propname]
    last_updated    = _in['last_updated']

    if  buffer_for_derivative_calc.get(coinID) is None:
        init_buffer_for_coinID_and_propname(coinID, propname, y)
        return _in

    if  buffer_for_derivative_calc[coinID].get(propname) is None:
        init_buffer_for_propname(coinID, propname, y)
        return _in

    prev_y = buffer_for_derivative_calc[coinID][propname]['y'][-1]
    buffer_for_derivative_calc[coinID][propname]['y'].append(y)

    if  buffer_for_derivative_calc[coinID][propname]['last_updated'] == last_updated and \
        prev_y == y:
        return _in

    buffer_for_derivative_calc[coinID][propname]['last_updated'] = last_updated

    valque = buffer_for_derivative_calc[coinID][propname]['y']
    y1000 = float(math.fsum(valque)) / len(valque)

    der = float(y - prev_y)
    _in[propname + "_derivative"] = der

    # calculate the percent change against the price
    pder = (der / y) * 100 \
        if y > 0 else None
    _in[propname + "_percent_derivative"] = pder

    # calculate the percent change against the AVG price
    pder = (der / y1000) * 100 \
        if y1000 > 0 else None
    _in[propname + "_smooth_percent_derivative"] = pder

    return _in