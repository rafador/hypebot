import settings
from elasticsearch import Elasticsearch
import math

es = Elasticsearch(settings.ELASTICSEARCH_CONNECT_STRING)


es_data_buffer_for_last_val = None

def query_es_last_values_into_buffer():
    global es_data_buffer_for_last_val
    if es_data_buffer_for_last_val is not None:
        return

    es_data_buffer_for_last_val = {}

    # try:
    res = es.search(index=settings.ES_INDEX_CMC,
                    body={
                        "query": {
                            "bool": {
                                "must": [
                                    {
                                        "match_all": {}
                                    }
                                ]
                            }
                        },
                      "size": 2000,
                      "sort": [
                        {
                          "@timestamp": {
                            "order": "desc"
                          }
                        }
                      ]
                    })

    for hit in res['hits']['hits']:
        coin_id = hit['_source']['id']
        last_updated = hit['_source']['last_updated']

        if es_data_buffer_for_last_val.get(coin_id) is None:
            es_data_buffer_for_last_val[coin_id] = hit['_source']
            continue
        # overwrite with a newer entry
        if es_data_buffer_for_last_val[coin_id]['last_updated'] < last_updated:
            es_data_buffer_for_last_val[coin_id] = hit['_source']


def query_es_for_cmc_last_value(coinID, propname):
    query_es_last_values_into_buffer()
    try:
        # res = es.search(index=settings.ES_INDEX_CMC,
        #                 body={
        #                   "query": {
        #                     "bool": {
        #                       "must": [
        #                         {
        #                           "query_string": {
        #                             "query": f"id.keyword: {coinID}",
        #                             "analyze_wildcard": "false"
        #                           }
        #                         }
        #                       ]
        #                     }
        #                   },
        #                   "size": 1,
        #                   "sort": [
        #                     {
        #                       "@timestamp": {
        #                         "order": "desc"
        #                       }
        #                     }
        #                   ]
        #                 })
        # return res['hits']['hits'][0]['_source'][propname]
        global es_data_buffer_for_last_val
        es_data_buffer_for_last_val[coinID][propname]
    except:
        return None


es_data_buffer_for_max = {}

def query_es_for_cmc_max_value(coin_id, propname, deepness):
    # global es_data_buffer_for_max
    # try:
    #     return es_data_buffer_for_max[f"{coin_id}_{propname}_{deepness}"]
    # except:
    #     if es_data_buffer_for_max.get(deepness) is None:
    #         es_data_buffer_for_max[deepness] = {}
    #     else:
    #         if es_data_buffer_for_max.get(coin_id) is None:
    #             es_data_buffer_for_max[deepness][coin_id] = {}

    try:
        minutes_in_the_past = math.ceil((deepness * settings.MARKET_REFRESH_RATE) / 60)
        res = es.search(index=settings.ES_INDEX_CMC,
                        body={
                          "query": {
                            "bool": {
                              "must": [
                                {
                                  "query_string": {
                                    "query": f"id.keyword: {coin_id}",
                                    "analyze_wildcard": "false"
                                  }
                                },
                                {
                                  "range": {
                                    "@timestamp": {
                                        "gte": f"now-{minutes_in_the_past}m",
                                        "format": "epoch_millis"
                                    }
                                  }
                                }
                              ]
                            }
                          },
                          "size": 1,
                          "sort": [
                            {
                              f"{propname}": {
                                "order": "desc"
                              }
                            }
                          ]
                        })
        return res['hits']['hits'][0]['_source'][propname]
    except:
        return None

def query_es_for_cmc_min_value(coinID, propname, deepness):
    try:
        minutes_in_the_past = math.ceil((deepness * settings.MARKET_REFRESH_RATE) / 60)
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
                                },
                                {
                                  "range": {
                                    "@timestamp": {
                                        "gte": f"now-{minutes_in_the_past}m",
                                        "format": "epoch_millis"
                                    }
                                  }
                                }
                              ]
                            }
                          },
                          "size": 1,
                          "sort": [
                            {
                              f"{propname}": {
                                "order": "asc"
                              }
                            }
                          ]
                        })
        return res['hits']['hits'][0]['_source'][propname]
    except:
        return None