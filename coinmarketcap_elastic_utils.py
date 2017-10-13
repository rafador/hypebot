import settings
from elasticsearch import Elasticsearch
import math

es = Elasticsearch(settings.ELASTICSEARCH_CONNECT_STRING)

def query_es_for_cmc_last_value(coinID, propname):
    try:
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
        return res['hits']['hits'][0]['_source'][propname]
    except:
        return None

def query_es_for_cmc_max_value(coinID, propname, deepness):
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
                                        "gte": f"now-{minutes_in_the_past}d",
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