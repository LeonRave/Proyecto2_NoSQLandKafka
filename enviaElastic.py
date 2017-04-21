from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime
import json

consumer = KafkaConsumer('elastic_topic')
#es = Elasticsearch(["localhost:9200"])
es = Elasticsearch(["http://search-beevagrad-yzavdnk3vgybj33teqgucq7ray.us-east-1.es.amazonaws.com:80"])

print(">> Escuchando Twiiters en topico elastic_topic:")

for msg in consumer:
	
	raw = msg.value.decode('utf8')
	doc = json.loads(raw)
	
	try:
		print(doc['entities']['user_mentions'][0]['name'])
	except:
		print('Usuario sin nombre')

	try:
		res = es.index(index="leonkafka", doc_type="mensajes", body=doc)
		print("Estado mensaje: ",res['created'])
	except:
		print('Error al enviar')

#result = es.bulk(index="testito", doc_type="tests", body=doc)





