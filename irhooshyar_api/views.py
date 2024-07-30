from django.http import JsonResponse
from elasticsearch import Elasticsearch

es_url = "http://192.168.50.8:9200"
client = Elasticsearch(es_url, timeout=30)
bucket_size = 1000
search_result_size = 100

def search_document(request, text):
    if text.replace(" ", "") == "":
        res_query = {"match_all": {}}
    else:
        res_query = {"match_phrase": {"content": text}}

    index_name = "hooshyar_document_index"

    response = client.search(index=index_name,
                             _source_includes=['name', 'category', 'datetime'],
                             request_timeout=40,
                             query=res_query,
                             sort=[{"datetime.year": {"order": "desc"}},
                                   {"datetime.month.number": {"order": "desc"}},
                                   {"datetime.day.number": {"order": "desc"}}],
                             size=10)

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

    return JsonResponse({"result": result, 'total_hits': total_hits})
