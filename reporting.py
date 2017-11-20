#! /python3

from cassandra.cluster import Cluster
import numpy as np
import csv

cluster = Cluster(['127.0.0.1'])
session = cluster.connect("e17")

# fonctions

# distance entre deux points à partir de la latitude et de la longitude
def dist(lon1,lat1,lon2,lat2) :	
	Rt = 6371008
	d = np.sqrt(
		((lon1-lon2)*np.cos((lat1+lat2)/2/180*np.pi))**2 + (lat1-lat2)**2
	)/180*np.pi*Rt
	return d

# distance au carré entre deux trajets
def dist_trajet(t1,t2) :
	dist_depart = dist(t1[1],t1[0],t2[1],t2[0])
	dist_arrivee = dist(t1[3],t1[2],t2[3],t2[2])
	return dist_depart**2 + dist_arrivee**2

		
# kmeans
# au hasard dans l'espace (classes vides), parmi les points  
# critère d'un kmean -> on minimise la somme des écarts au carré

# par rapport à un jour donné
reqA = "SELECT lat_depart,lon_depart,lat_arrivee,lon_arrivee FROM table_faits_temps WHERE annee = 2014 AND mois = 3 AND jour = 25;"
reqB = "SELECT lat_depart,lon_depart,lat_arrivee,lon_arrivee FROM table_faits_temps WHERE annee = 2013 AND mois = 8 AND jour = 15;"

# par rapport à une heure donnée, tendance ?
req1 = "SELECT lat_depart,lon_depart,lat_arrivee,lon_arrivee FROM table_faits_heure WHERE annee = 2013 AND heure = 17;"
req2 = "SELECT lat_depart,lon_depart,lat_arrivee,lon_arrivee FROM table_faits_heure WHERE annee = 2013 AND heure = 23;"
req3 = "SELECT lat_depart,lon_depart,lat_arrivee,lon_arrivee FROM table_faits_heure WHERE annee = 2013 AND heure = 8;"
req4 = "SELECT lat_depart,lon_depart,lat_arrivee,lon_arrivee FROM table_faits_heure WHERE annee = 2013 AND heure = 2;"

# par rapport à un taxiste
reqD = "select lat_depart,lon_depart,lat_arrivee,lon_arrivee from table_faits_taxi where taxi_id=20000454;"


def kmeans(req) :
	centroids_prec = [np.array([0,0,0,0]),np.array([0,0,0,0]),np.array([0,0,0,0])] # nécessaire pour rentrer dans la boucle

	# initialisation des centroids de manière aléatoire
	centroids = [None,None,None]
	for i in range(0,3) :
		m = 1
		for d in session.execute(req) :
			d =  np.array([d.lat_depart,d.lon_depart,d.lat_arrivee,d.lon_arrivee])
			u = np.random.uniform()
			if u < m :
				m = u
				centroids[i] = d
	i = 0	

	import IPython
	while any([any(centroids[i] != centroids_prec[i]) for i in range(len(centroids))]): # tant que ça ne converge pas
		centroids_prec = centroids
		sommes = [[0,np.array([0.,0.,0.,0.])],[0,np.array([0.,0,0,0])],[0,np.array([0.,0,0,0])]]
		for xi in session.execute(req) :
			xi =  np.array([xi.lat_depart,xi.lon_depart,xi.lat_arrivee,xi.lon_arrivee])
			d = [dist_trajet(xi,c) for c in centroids]
			k = d.index(min(d))
			sommes[k][0] += 1
			sommes[k][1] += xi
		centroids = [s[1]/s[0] for s in sommes] 
		#IPython.embed() # permet de faire du debugging
		i += 1

	print("Centroids :")
	return [centroids,i,req]

# appel de la fonction pour différentes requêtes
#k_25_3_2014 = kmeans(reqA)
#print(k_25_3_2014)

#k_15_8_2013 = kmeans(reqB)
#print(k_15_8_2013)

#k_31_12_2013 = kmeans(reqC)
#print(k_31_12_2013)

taxiste = kmeans(reqD)
print(taxiste)

#heure_17 = kmeans(req1)
#print(heure_17)

#heure_23 = kmeans(req2)
#print(heure_23)

#heure_8 = kmeans(req3)
#print(heure_8)

# on importe ensuite ces données dans R pour faire un graphique


# nuages de points
reqA = "SELECT lat_depart,lon_depart FROM table_faits_temps2 WHERE annee = 2013 AND mois = 10 AND jour = 25;"

reqB1 = "SELECT lat_depart,lon_depart FROM table_faits_temps WHERE annee = 2013 AND mois = 8 AND jour = 15;"
reqB2 = "SELECT lat_arrivee,lon_arrivee FROM table_faits_temps WHERE annee = 2013 AND mois = 8 AND jour = 15;"

reqD1 = "SELECT lat_depart,lon_depart FROM table_faits_heure WHERE annee = 2013 AND heure = 9 and mois = 9;"
reqD2 = "SELECT lat_depart,lon_depart FROM table_faits_heure WHERE annee = 2013 AND heure = 18 and mois = 9;"

# on écrit le résultat des requêtes dans un fichier .csv pour ensuite faire le graphique sous R
with open('out_depart_9h_sept.csv', 'w') as out:
	writer = csv.writer(out)
	for xi in session.execute(reqD1) :
		writer.writerow([xi.lat_depart,xi.lon_depart])
		
with open('out_depart_18h_sept.csv', 'w') as out:
	writer = csv.writer(out)
	for xi in session.execute(reqD2) :
		writer.writerow([xi.lat_depart,xi.lon_depart])
