
import pymongo as mon
from kafka import KafkaConsumer
import json



consumer = KafkaConsumer('mongo_topic')
mongoClient = mon.MongoClient('172.17.0.2',27017)

# Conexión a la base de datos
db = mongoClient.grad
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
