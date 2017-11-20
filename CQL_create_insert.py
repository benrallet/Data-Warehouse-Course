#! /python3

import csv
import datetime
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1']) # connexion au cluster
session = cluster.connect("e17")

# création des tables des faits

reqA = 'CREATE TABLE table_faits_temps (annee int, mois int, jour int, heure int, minute int, seconde int, trip_id bigint, taxi_id bigint, lat_depart float, lon_depart float, lat_arrivee float, lon_arrivee float, call_type text, origin_call text, origin_stand text, day_type text, primary key((annee,mois,jour),heure,minute,seconde,trip_id));'		
reqB = 'CREATE TABLE table_faits_arrivee (id_arrivee int, annee int, mois int, jour int, heure int, minute int, seconde int, trip_id bigint, taxi_id bigint, lat_depart float, lon_depart float, lat_arrivee float, lon_arrivee float, call_type text, origin_call text, origin_stand text, day_type text, primary key((id_arrivee,annee),mois,jour,heure,minute,seconde,trip_id));'		
reqC = 'CREATE TABLE table_faits_depart (id_depart int, annee int, mois int, jour int, heure int, minute int, seconde int, trip_id bigint, taxi_id bigint, lat_depart float, lon_depart float, lat_arrivee float, lon_arrivee float, call_type text, origin_call text, origin_stand text, day_type text, primary key((id_depart,annee),mois,jour,heure,minute,seconde,trip_id));'		
reqD = 'CREATE TABLE table_faits_heure (heure int, annee int, mois int, jour int, minute int, seconde int, trip_id bigint, taxi_id bigint, lat_depart float, lon_depart float, lat_arrivee float, lon_arrivee float, call_type text, origin_call text, origin_stand text, day_type text, primary key((heure, annee),mois,jour,minute,seconde,trip_id));'		
reqE = 'CREATE TABLE table_faits_taxi (taxi_id bigint, annee int, mois int, jour int, heure int, minute int, seconde int, trip_id bigint, lat_depart float, lon_depart float, lat_arrivee float, lon_arrivee float, call_type text, origin_call text, origin_stand text, day_type text, primary key(taxi_id,annee,mois,jour,heure,minute,seconde,trip_id));'		
reqF = 'CREATE TABLE table_faits_day_type (day_type text, annee int, mois int, jour int, heure int, minute int, seconde int, trip_id bigint, taxi_id bigint, lat_depart float, lon_depart float, lat_arrivee float, lon_arrivee float, call_type text, origin_call text, origin_stand text, primary key((day_type,annee,mois),heure,minute,seconde,trip_id));'		

session.execute(reqA)
session.execute(reqB)
session.execute(reqC)
session.execute(reqD)
session.execute(reqE)
session.execute(reqF)


# ouverture du fichier
fichier = open ('/train.csv','r')
csv = csv.reader(fichier,delimiter=',')

# fonctions

# Renvoie les coordonnées GPS du départ et de l'arrivée d'un trajet
def tronc_geom(l) :		
	geom = l[8]
	if geom != "POLYLINE" and geom != "[]" :
		table_coord = geom.split("]")
		depart= table_coord[0][1:] + "]"
		depart = depart.split(',')
		depart_lon = float(depart[0][2:])
		depart_lat = float(depart[1][:len(depart[1])-2])
		
		destination = table_coord[len(table_coord)-3][1:] + "]"
		destination = destination.split(',')
		destination_lon = float(destination[0][2:])
		destination_lat = float(destination[1][:len(destination[1])-2])
		return [depart_lon*(-1),depart_lat,destination_lon*(-1),destination_lat]
	else :
		return None

# Renvoie un id de dallage
def dallage(lon,lat):
	if (lat < 36.05 or lat > 45.05 or lon < -9.50 or lon > -3.23) :
		return 11 # valeur aberrante
	# longitude
	if(lon <= -8.57 and lon > -8.61) :
		id_dall = 0
	else :
		if (lon <= -8.61 and lon > -8.65) :
			id_dall = 3
		else :
			if (lon <= -8.65 and lon > -8.69) :
				id_dall = 6
			else :
				return 10 # valeur extra-porto mais pas aberrant
	
	# latitude
	if(lat > 41.09 and lat < 41.14) :
		id_dall += 1
	else :
		if(lat >= 41.14 and lat < 41.19) :
			id_dall +=2
		else :
			if(lat >= 41.19 and lat < 41.24) :
				id_dall += 3
			else :
				return 10 # valeur extra-porto mais pas aberrant

	return id_dall

# Séparation des différentes composantes du timestamp
def time_fun(l) :
	timestamp = l
	if timestamp != "TIMESTAMP" :
		year,month,day,hour,minute,seconde = datetime.datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d-%H-%M-%S').split('-')
		return [year,month,day,hour,minute,seconde]

# lecture de toutes les lignes et insertion dans les différentes tables
i = 0
for l in csv :
	if i != 0 : # pas la ligne d'en-tête
		print(i)
		
		# récupération des différents attributs
		trip_id = l[0]
		call_type = l[1]
		origin_call = l[2]
		origin_stand = l[3]
		taxi_id = l[4]
		time = time_fun(l[5])
		annee = int(time[0])
		mois = int(time[1])
		jour = int(time[2])
		heure = int(time[3])
		minute = int(time[4])
		seconde = int(time[5])
		day_type = l[6]
		if tronc_geom(l) != None :
		
			id_depart = dallage(tronc_geom(l)[0],tronc_geom(l)[1])
			id_arrivee = dallage(tronc_geom(l)[2],tronc_geom(l)[3])
			
			lat_depart = tronc_geom(l)[1]
			lon_depart = tronc_geom(l)[0]
			lat_arrivee = tronc_geom(l)[3]
			lon_arrivee = tronc_geom(l)[2]
			
			# creation des tables
			
			reqA = "INSERT INTO table_faits_temps (annee, mois, jour, heure, minute, seconde, trip_id, taxi_id, lat_depart, lon_depart, lat_arrivee, lon_arrivee, call_type, origin_call, origin_stand, day_type) values (%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,'%s','%s','%s','%s');" %(annee,mois,jour,heure,minute,seconde,int(trip_id),int(taxi_id),lat_depart,lon_depart,lat_arrivee,lon_arrivee,call_type,origin_call,origin_stand,day_type)
			reqB = "INSERT INTO table_faits_arrivee (id_arrivee, annee, mois, jour, heure, minute, seconde, trip_id, taxi_id, lat_depart, lon_depart, lat_arrivee, lon_arrivee, call_type, origin_call, origin_stand, day_type) values (%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,'%s','%s','%s','%s');" %(id_arrivee,annee,mois,jour,heure,minute,seconde,int(trip_id),int(taxi_id),lat_depart,lon_depart,lat_arrivee,lon_arrivee,call_type,origin_call,origin_stand,day_type)
			reqC = "INSERT INTO table_faits_depart (id_depart, annee, mois, jour, heure, minute, seconde, trip_id, taxi_id, lat_depart, lon_depart, lat_arrivee, lon_arrivee, call_type, origin_call, origin_stand, day_type) values (%d,%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,'%s','%s','%s','%s');" %(id_depart,annee,mois,jour,heure,minute,seconde,int(trip_id),int(taxi_id),lat_depart,lon_depart,lat_arrivee,lon_arrivee,call_type,origin_call,origin_stand,day_type)
			reqD = "INSERT INTO table_faits_heure (heure, annee, mois, jour, minute, seconde, trip_id, taxi_id, lat_depart, lon_depart, lat_arrivee, lon_arrivee, call_type, origin_call, origin_stand, day_type) values (%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,'%s','%s','%s','%s');" %(heure,annee,mois,jour,minute,seconde,int(trip_id),int(taxi_id),lat_depart,lon_depart,lat_arrivee,lon_arrivee,call_type,origin_call,origin_stand,day_type)
			reqE = "INSERT INTO table_faits_taxi (taxi_id, annee, mois, jour, heure, minute, seconde, trip_id, lat_depart, lon_depart, lat_arrivee, lon_arrivee, call_type, origin_call, origin_stand, day_type) values (%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,'%s','%s','%s','%s');" %(int(taxi_id),annee,mois,jour,heure,minute,seconde,int(trip_id),lat_depart,lon_depart,lat_arrivee,lon_arrivee,call_type,origin_call,origin_stand,day_type)
			reqF = "INSERT INTO table_faits_day_type (day_type, annee, mois, jour, heure, minute, seconde, trip_id, taxi_id, lat_depart, lon_depart, lat_arrivee, lon_arrivee, call_type, origin_call, origin_stand) values ('%s',%d,%d,%d,%d,%d,%d,%d,%d,%f,%f,%f,%f,'%s','%s','%s');" %(day_type,annee,mois,jour,heure,minute,seconde,int(trip_id),int(taxi_id),lat_depart,lon_depart,lat_arrivee,lon_arrivee,call_type,origin_call,origin_stand)
			
			session.execute(reqA)
			session.execute(reqB)
			session.execute(reqC)
			session.execute(reqD)
			session.execute(reqE)
			session.execute(reqF)

		i += 1 # permet de suivre l'avancée de l'insertion
		
	else :
		i += 1
