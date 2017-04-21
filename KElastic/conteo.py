import operator

s = " hola mundo mundito mundo de pacotilla "

def maximo(mensaje):
	dic = {}

	mensaje = str(mensaje).split(" ")
	for s in mensaje:
		if(s!=''):
			dic[s] = dic.get(s, 0) + 1
	#print(dic)

	dic = sorted(dic.items(), key=operator.itemgetter(1))
	test = list(dic[len(dic)-1])
	return str(test[0])+" "+str(test[1])


