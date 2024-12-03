from django.http import JsonResponse
from elasticsearch import Elasticsearch

es_url = "http://192.168.50.8:9200"
client = Elasticsearch(es_url, timeout=30)
bucket_size = 1000
search_result_size = 100


def process_agg(data):
    keys = [d["key"] for d in data if d["key"] != 0]
    if len(keys) == 0:
        return []
        
    min_key = min(keys)
    max_key = max(keys)

    sorted_keys = list(range(min_key, max_key + 1))

    result = []
    key_to_doc_count = {d["key"]: d["doc_count"] for d in data}

    for key in sorted_keys:
        doc_count = key_to_doc_count.get(key, 0)
        result.append({"key": key, "doc_count": doc_count})

    return result

def search_document(request, text):
    if text.replace(" ", "") == "":
        res_query = {"match_all": {}}
    else:
        res_query = {"match_phrase": {"content": text}}

    index_name = "hooshyar_document_index"
    res_agg = {
        "year-agg": {
            "terms": {
                "field": "datetime.year",
                "size": bucket_size
            }
        },
    }
    response = client.search(index=index_name,
                             _source_includes=['name', 'category', 'datetime'],
                             request_timeout=40,
                             query=res_query,
                             sort=[{"datetime.year": {"order": "desc"}},
                                   {"datetime.month.number": {"order": "desc"}},
                                   {"datetime.day.number": {"order": "desc"}}],
                             size=10,
                             aggregations=res_agg)

    result = response['hits']['hits']

    total_hits = response['hits']['total']['value']

    if total_hits == 10000:
        total_hits = client.count(body={"query": res_query}, index=index_name)['count']

    for i in range(result.__len__()):
        result[i]["_source"]["approval_reference_name"] = result[i]["_source"]["category"]
        date = "نامشخص"
        if result[i]['_source']['datetime'] is not None and result[i]['_source']['datetime']["year"] != 0:
            year = result[i]['_source']['datetime']["year"]
            month = result[i]['_source']['datetime']["month"]["number"]
            day = result[i]['_source']['datetime']["day"]["number"]
            date = str(year) + "/" + str(month) + "/" + str(day)

        result[i]["_source"]["approval_date"] = date

    year_agg = process_agg(response['aggregations']["year-agg"]["buckets"])

    return JsonResponse({"result": result, 'total_hits': total_hits,
                         "year_chart": year_agg})
