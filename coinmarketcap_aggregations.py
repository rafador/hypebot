import collections
import math

class Aggregations(object):

    def __init__(self):
        # TODO: pickle
        self.aggr_buffer = {}

    def add_avg_for_property(self, _in, propname, count):
        if _in[propname] is None:
            return _in

        val = float(_in[propname])
        symbol_id  = _in['id']
        propname_id = propname + "_avg" + str(count)
        count = int(count)

        if self.aggr_buffer.get(symbol_id) is None:
            self.aggr_buffer[symbol_id] = {}

        if self.aggr_buffer[symbol_id].get(propname_id) is None:
            self.aggr_buffer[symbol_id][propname_id] = collections.deque(count * [val], count)

        self.aggr_buffer[symbol_id][propname_id].append(val)

        # calc avg
        valque = self.aggr_buffer[symbol_id][propname_id]
        valavg = float(math.fsum(valque)) / len(valque)
        _in[propname_id] = valavg

        # calc difference between avg and value
        _in[propname_id + "_diff"] = val - valavg

        # calc percentual difference between avg and value
        _in[propname_id + "_percent_diff"] = (( val - valavg ) / valavg ) * 100 \
            if valavg != 0 else 0

        return _in

    def add_min_for_property(self, _in, propname, count):
        if _in[propname] is None:
            return _in

        val = float(_in[propname])
        symbol_id  = _in['id']
        propname_id = propname + "_min" + str(count)
        count = int(count)

        if self.aggr_buffer.get(symbol_id) is None:
            self.aggr_buffer[symbol_id] = {}

        if self.aggr_buffer[symbol_id].get(propname_id) is None:
            self.aggr_buffer[symbol_id][propname_id] = collections.deque(count * [val], count)

        self.aggr_buffer[symbol_id][propname_id].append(val)

        # calc min
        valque = self.aggr_buffer[symbol_id][propname_id]
        _in[propname_id] = min(valque)

        return _in

    def add_max_for_property(self, _in, propname, count):
        if _in[propname] is None:
            return _in

        val = float(_in[propname])
        symbol_id  = _in['id']
        propname_id = propname + "_max" + str(count)
        count = int(count)

        if self.aggr_buffer.get(symbol_id) is None:
            self.aggr_buffer[symbol_id] = {}

        if self.aggr_buffer[symbol_id].get(propname_id) is None:
            self.aggr_buffer[symbol_id][propname_id] = collections.deque(count * [val], count)

        self.aggr_buffer[symbol_id][propname_id].append(val)

        # calc MAX
        valque = self.aggr_buffer[symbol_id][propname_id]
        _in[propname_id] = max(valque)

        return _in

    def add_ceil_for_property(self, _in, propname):
        val = float(_in[propname])

        _in[propname + "_ceiled"] = math.ceil(val)

        return _in

    def add_floor_for_property(self, _in, propname):
        val = float(_in[propname])

        _in[propname + "_floored"] = math.floor(val)

        return _in



