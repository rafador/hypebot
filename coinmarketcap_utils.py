from datetime import datetime
from coinmarketcap_aggregations import *
import logging


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




buffer_for_derivative_calc = {}
def add_derivative_for_property(_in, propname):

    coinID = _in['id']
    x = _in['@timestamp']
    y = _in[propname]
    last_updated = _in['last_updated']


    if  buffer_for_derivative_calc.get(coinID) is None:
        buffer_for_derivative_calc[coinID] = {}
        buffer_for_derivative_calc[coinID][propname] = {}
        buffer_for_derivative_calc[coinID][propname]['y'] = y
        # buffer_for_derivative_calc[coinID][propname]['x'] = x
        buffer_for_derivative_calc[coinID][propname]['last_updated'] = '0'
        return _in

    if  buffer_for_derivative_calc[coinID].get(propname) is None:
        buffer_for_derivative_calc[coinID][propname] = {}
        buffer_for_derivative_calc[coinID][propname]['y'] = y
        # buffer_for_derivative_calc[coinID][propname]['x'] = x
        buffer_for_derivative_calc[coinID][propname]['last_updated'] = '0'
        return _in

    if  buffer_for_derivative_calc[coinID][propname]['last_updated'] == last_updated and \
        buffer_for_derivative_calc[coinID][propname]['y'] == y:
        return _in

    # prev_x = buffer_for_derivative_calc[coinID][propname]['x']
    prev_y      = buffer_for_derivative_calc[coinID][propname]['y']

    # buffer_for_derivative_calc[coinID][propname]['x'] = x
    buffer_for_derivative_calc[coinID][propname]['y'] = y
    buffer_for_derivative_calc[coinID][propname]['last_updated'] = last_updated

    der = float(y - prev_y) # / (x - prev_x).total_seconds()
    _in[propname + "_derivative"] = der

    # calculate the percent change against the price
    pder = (der / y) * 100 \
        if y > 0 else None
    _in[propname + "_percent_derivative"] = pder

    return _in