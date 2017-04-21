import re
import unicodedata

def eliminaURL(s):

	while(True):
		if s.find('http')<0:
			break
		
		else:
			n = s.find('http')
			i = 0
			try:
		    		while s[n+i]!=' ':
		        		i=i+1
			except:
		    		i=i   

			if s[n-1] == ' ':
		    		url = s[n:n+i]
		    		s = s.replace(s[n-1:n+i],'')
			else:
		    		url = s[n:n+i]
		    		s = s.replace(s[n:n+i],'')
	return s

def elimina_acentos(s):
	return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))

def elimina_puntuacion(s):
	return re.sub('\W+', ' ',s)

def pasa_minusculas(s):
	return s.lower()
