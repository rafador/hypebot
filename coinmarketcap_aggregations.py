import collections
import math
from coinmarketcap_elastic_utils import *

def calc_percent_ratio(val, base):
    return (val / abs(base)) * 100 \
        if base != 0 else None

def calc_absolute_val(val):
    return abs(val) \
        if val is not None else None


class Aggregations(object):

    def __init__(self):
        self.aggr_buffer = {}

    def add_avg_for_property(self, _in, propname, deepness):
        if _in[propname] is None:
            return _in

        val = float(_in[propname])
        symbol_id  = _in['id']
        propname_id = propname + "_avg" + str(deepness)
        deepness = int(deepness)

        if self.aggr_buffer.get(symbol_id) is None:
            self.aggr_buffer[symbol_id] = {}

        if self.aggr_buffer[symbol_id].get(propname_id) is None:
            last_from_es = query_es_for_cmc_last_value(symbol_id, propname_id)
            if last_from_es is not None:
                self.aggr_buffer[symbol_id][propname_id] = collections.deque(deepness * [last_from_es], deepness)
            else:
                self.aggr_buffer[symbol_id][propname_id] = collections.deque(deepness * [val], deepness)

        self.aggr_buffer[symbol_id][propname_id].append(val)

        # calc avg
        valque = self.aggr_buffer[symbol_id][propname_id]
        valavg = float(math.fsum(valque)) / len(valque)
        _in[propname_id] = valavg

        # calc difference between avg and value
        _in[propname_id + "_diff"] = val - valavg

        # calc percentual difference between avg and value
        _in[propname_id + "_percent_diff"] = calc_percent_ratio(val - valavg, valavg )

        return _in

    def add_min_for_property(self, _in, propname, deepness):
        if _in[propname] is None:
            return _in

        val = float(_in[propname])
        symbol_id  = _in['id']
        propname_id = propname + "_min" + str(deepness)
        deepness = int(deepness)

        if self.aggr_buffer.get(symbol_id) is None:
            self.aggr_buffer[symbol_id] = {}

        if self.aggr_buffer[symbol_id].get(propname_id) is None:
            min_from_es = query_es_for_cmc_min_value(symbol_id, propname_id, deepness)
            if min_from_es is not None:
                self.aggr_buffer[symbol_id][propname_id] = collections.deque(deepness * [min_from_es], deepness)
            else:
                self.aggr_buffer[symbol_id][propname_id] = collections.deque(deepness * [val], deepness)

        self.aggr_buffer[symbol_id][propname_id].append(val)

        # calc min
        valque = self.aggr_buffer[symbol_id][propname_id]
        _in[propname_id] = min(valque)

        return _in

    def add_max_for_property(self, _in, propname, deepness):
        if _in[propname] is None:
            return _in

        val = float(_in[propname])
        symbol_id  = _in['id']
        propname_id = propname + "_max" + str(deepness)
        deepness = int(deepness)

        if self.aggr_buffer.get(symbol_id) is None:
            self.aggr_buffer[symbol_id] = {}

        if self.aggr_buffer[symbol_id].get(propname_id) is None:
            max_from_es = query_es_for_cmc_min_value(symbol_id, propname_id, deepness)
            if max_from_es is not None:
                self.aggr_buffer[symbol_id][propname_id] = collections.deque(deepness * [max_from_es], deepness)
            else:
                self.aggr_buffer[symbol_id][propname_id] = collections.deque(deepness * [val], deepness)

        self.aggr_buffer[symbol_id][propname_id].append(val)

        # calc MAX
        valque = self.aggr_buffer[symbol_id][propname_id]
        _in[propname_id] = max(valque)

        return _in

    def _normalize_value(self, val, min, max):
        if  val is None or \
            min is None or \
            max is None:
            return None
        newzero = min
        newmax  = max - newzero
        # scale to [0 1] range
        norm = (val - newzero) * (1 / newmax) \
            if newmax is not None and newmax != 0 else None
        return norm

    def add_normalized_for_property(self, _in, propname, deepness):
        propname_id = f"{propname}_normalized{str(deepness)}"

        _in = agg.add_min_for_property(_in, propname, deepness)
        _in = agg.add_max_for_property(_in, propname, deepness)
        val = _in[propname]
        min = _in[f"{propname}_min{deepness}"]
        max = _in[f"{propname}_max{deepness}"]

        _in[propname_id] = \
            self._normalize_value(val, min, max)

        return _in

    def add_ceil_for_property(self, _in, propname):
        val = float(_in[propname])

        _in[propname + "_ceiled"] = math.ceil(val)

        return _in

    def add_floor_for_property(self, _in, propname):
        val = float(_in[propname])

        _in[propname + "_floored"] = math.floor(val)

        return _in

agg = Aggregations()

