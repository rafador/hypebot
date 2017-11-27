from coinmarketcap_aggregations import *

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

    deepness = str(200)
    # rank metrics
    e = agg.add_min_for_property(e, "rank", deepness)
    e = agg.add_max_for_property(e, "rank", deepness)
    e = agg.add_avg_for_property(e, "rank", deepness)
    e = agg.add_min_for_property(e, "rank_avg" + deepness, deepness)
    e = agg.add_ceil_for_property(e, "rank_avg" + deepness + "_min" + deepness)
    e = agg.add_floor_for_property(e, "rank_avg" + deepness + "_min" + deepness)
    e = agg.add_max_for_property(e, "rank_avg" + deepness, deepness)
    e = agg.add_ceil_for_property(e, "rank_avg" + deepness + "_max" + deepness)
    e = agg.add_floor_for_property(e, "rank_avg" + deepness + "_max" + deepness)
    # price
    e = agg.add_avg_for_property(e, "price_btc", deepness)
    e = agg.add_avg_for_property(e, "price_usd", deepness)
    # market cap
    e = agg.add_avg_for_property(e, "market_cap_usd", deepness)
    e = agg.add_min_for_property(e, "market_cap_usd", deepness)
    e = agg.add_max_for_property(e, "market_cap_usd", deepness)
    e = agg.add_avg_for_property(e, "market_cap_btc", deepness)
    e = agg.add_min_for_property(e, "market_cap_btc", deepness)
    e = agg.add_max_for_property(e, "market_cap_btc", deepness)

    deepness = str(1000)
    # rank metrics
    e = agg.add_min_for_property(e, "rank", deepness)
    e = agg.add_max_for_property(e, "rank", deepness)
    e = agg.add_avg_for_property(e, "rank", deepness)
    e = agg.add_min_for_property(e, "rank_avg" + deepness, deepness)
    e = agg.add_ceil_for_property(e, "rank_avg" + deepness + "_min" + deepness)
    e = agg.add_floor_for_property(e, "rank_avg" + deepness + "_min" + deepness)
    e = agg.add_max_for_property(e, "rank_avg" + deepness, deepness)
    e = agg.add_ceil_for_property(e, "rank_avg" + deepness + "_max" + deepness)
    e = agg.add_floor_for_property(e, "rank_avg" + deepness + "_max" + deepness)
    # volume
    e = agg.add_avg_for_property(e, "volume_percent_of_market_cap", deepness)
    # price
    e = agg.add_avg_for_property(e, "price_btc", deepness)
    e = agg.add_avg_for_property(e, "price_usd", deepness)
    # market cap
    e = agg.add_avg_for_property(e, "market_cap_usd", deepness)
    e = agg.add_min_for_property(e, "market_cap_usd", deepness)
    e = agg.add_max_for_property(e, "market_cap_usd", deepness)
    e = agg.add_avg_for_property(e, "market_cap_btc", deepness)
    e = agg.add_min_for_property(e, "market_cap_btc", deepness)
    e = agg.add_max_for_property(e, "market_cap_btc", deepness)

    # cascading aggregations
    # 10 days
    deepness = 10 * 24 * 60
    e = agg.add_normalized_for_property(e, "24h_volume_usd", deepness)
    e = add_derivative_for_property(e, f"24h_volume_usd_normalized{deepness}")

    return e


buffer_for_derivative_calc = {}

def init_buffer_for_coinID_and_propname(coinID, propname, y, last_updated, smoothness):
    buffer_for_derivative_calc[coinID] = {}
    init_buffer_for_propname(coinID, propname, y, last_updated, smoothness)

def init_buffer_for_propname(coinID, propname, y, last_updated, smoothness):
    buffer_for_derivative_calc[coinID][propname] = {}
    # try query from elastic
    last_avg_from_es =  query_es_for_cmc_last_value(coinID, f"{propname}_avg{smoothness}")
    last_from_es = query_es_for_cmc_last_value(coinID, f"{propname}")
    # find the best last known value for Y
    base = y
    if last_from_es is not None:
        base = last_from_es
    if last_avg_from_es is not None:
        base = last_avg_from_es
    buffer_for_derivative_calc[coinID][propname]['y'] = collections.deque(smoothness * [base], smoothness)
    buffer_for_derivative_calc[coinID][propname]['last_updated'] = last_updated

def add_derivative_for_property(_in, propname):
    y = _in[propname]
    if y is None:
        return _in
    coinID          = _in['id']
    last_updated    = _in['last_updated']
    smoothness = 1000

    if  buffer_for_derivative_calc.get(coinID) is None:
        init_buffer_for_coinID_and_propname(coinID, propname, y, last_updated, smoothness)

    if  buffer_for_derivative_calc[coinID].get(propname) is None:
        init_buffer_for_propname(coinID, propname, y, last_updated, smoothness)

    prev_y = buffer_for_derivative_calc[coinID][propname]['y'][-1]
    buffer_for_derivative_calc[coinID][propname]['y'].append(y)

    # persist also the basis for the "smooth" derivative
    valque = buffer_for_derivative_calc[coinID][propname]['y']
    y1000 = float(math.fsum(valque)) / len(valque)
    _in[f"{propname}_avg{smoothness}"] = y1000

    # don't set derivatives, if no change is detected
    if  buffer_for_derivative_calc[coinID][propname]['last_updated'] == last_updated and \
        prev_y == y:
        return _in

    buffer_for_derivative_calc[coinID][propname]['last_updated'] = last_updated

    der = float(y - prev_y)
    _in[f"{propname}_derivative"] = der
    _in[f"{propname}_derivative_magnitude"] = calc_absolute_val(der)

    # calculate the percent change against the price
    pder = calc_percent_ratio(der, y)
    _in[f"{propname}_percent_derivative"] = pder
    _in[f"{propname}_percent_derivative_magnitude"] = calc_absolute_val(pder)

    # calculate the percent change against the AVG price
    spder = calc_percent_ratio(der, y1000)
    _in[f"{propname}_smooth_percent_derivative"] = spder
    _in[f"{propname}_smooth_percent_derivative_magnitude"] = calc_absolute_val(spder)

    return _in


