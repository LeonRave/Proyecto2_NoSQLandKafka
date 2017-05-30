from pymongo import MongoClient
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('mongo_topic')
client = MongoClient('54.174.5.92', 27017)

# Conexión a la base de datos
db = client.grad
# Selecciona la coleccion
leonkafka = db.leonkafka

for msg in consumer:
	
	raw = msg.value.decode('utf8')
	doc = json.loads(raw)
	
	try:
                print("Insertando metadata de: "+str(doc['entities']['user_mentions'][0]['name']))
	except:
		print('Usuario sin nombre')
         
	try:
		res = leonkafka.insert_one(doc)
	except:
		print("Error al insertar a mongo")
