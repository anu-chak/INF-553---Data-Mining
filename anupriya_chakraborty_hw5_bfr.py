import sys
from sklearn.cluster import KMeans
import numpy as np
import random
import time
import collections

start= time.time()

def update_DS_statistics(pointid, newpoint, cluster_key):
	# Points
	DS_statistics[cluster_key][0].append(pointid)
	# N
	DS_statistics[cluster_key][1]= DS_statistics[cluster_key][1]+1
	for i in range(0,d):
		#SUM
		DS_statistics[cluster_key][2][i]= DS_statistics[cluster_key][2][i]+newpoint[i]
		#SUMSQ
		DS_statistics[cluster_key][3][i]= DS_statistics[cluster_key][3][i]+newpoint[i]**2
	#STDDEV
	DS_statistics[cluster_key][4]= np.sqrt((DS_statistics[cluster_key][3][:]/DS_statistics[cluster_key][1]) - (np.square(DS_statistics[cluster_key][2][:])/(DS_statistics[cluster_key][1]**2)))
	#CENTROID
	DS_statistics[cluster_key][5]= DS_statistics[cluster_key][2]/DS_statistics[cluster_key][1]


def update_CS_statistics(pointid, newpoint, cluster_key):
	# Points
	CS_statistics[cluster_key][0].append(pointid)
	# N
	CS_statistics[cluster_key][1]= CS_statistics[cluster_key][1]+1
	for i in range(0,d):
		#SUM
		CS_statistics[cluster_key][2][i]= CS_statistics[cluster_key][2][i]+newpoint[i]
		#SUMSQ
		CS_statistics[cluster_key][3][i]= CS_statistics[cluster_key][3][i]+newpoint[i]**2
	#STDDEV
	CS_statistics[cluster_key][4]= np.sqrt((CS_statistics[cluster_key][3][:]/CS_statistics[cluster_key][1]) - (np.square(CS_statistics[cluster_key][2][:])/(CS_statistics[cluster_key][1]**2)))
	#CENTROID
	CS_statistics[cluster_key][5]= CS_statistics[cluster_key][2]/CS_statistics[cluster_key][1]

def merge_CS_clusters(key1, key2):
	# Points
	CS_statistics[key1][0].extend(CS_statistics[key2][0])
	# N
	CS_statistics[key1][1]= CS_statistics[key1][1]+CS_statistics[key2][1]
	for i in range(0,d):
		#SUM
		CS_statistics[key1][2][i]= CS_statistics[key1][2][i]+CS_statistics[key2][2][i]
		#SUMSQ
		CS_statistics[key1][3][i]= CS_statistics[key1][3][i]+CS_statistics[key2][3][i]
	#STDDEV
	CS_statistics[key1][4]= np.sqrt((CS_statistics[key1][3][:]/CS_statistics[key1][1]) - (np.square(CS_statistics[key1][2][:])/(CS_statistics[key1][1]**2)))
	#CENTROID
	CS_statistics[key1][5]= CS_statistics[key1][2]/CS_statistics[key1][1]

def merge_with_DS_cluster(keyCS, keyDS):
	# Points
	DS_statistics[keyDS][0].extend(CS_statistics[keyCS][0])
	# N
	DS_statistics[keyDS][1]= DS_statistics[keyDS][1]+CS_statistics[keyCS][1]
	for i in range(0,d):
		#SUM
		DS_statistics[keyDS][2][i]= DS_statistics[keyDS][2][i]+CS_statistics[keyCS][2][i]
		#SUMSQ
		DS_statistics[keyDS][3][i]= DS_statistics[keyDS][3][i]+CS_statistics[keyCS][3][i]
	#STDDEV
	DS_statistics[keyDS][4]= np.sqrt((DS_statistics[keyDS][3][:]/DS_statistics[keyDS][1]) - (np.square(DS_statistics[keyDS][2][:])/(DS_statistics[keyDS][1]**2)))
	#CENTROID
	DS_statistics[keyDS][5]= DS_statistics[keyDS][2]/DS_statistics[keyDS][1]


data_file = sys.argv[1]
num_clusters = int(sys.argv[2])
out_file = sys.argv[3]

file = open(data_file, "r")
data = np.array(file.readlines())
file.close()

# for i in data:
# 	print(str(i))

# ================================= Sampling 20% data initial load, cluster and move isolated points to RS =================================

lines= int(len(data)*0.2)
lastiter_lines= len(data)-lines*4

initial_sample = np.random.choice(a= data, size= lines, replace= False)
# initial_sample = np.random.choice(a= data, size= 20, replace= False)

point_ids= {} # ctr -> pointid
pointid_to_point= {} # pointid -> point
point_to_pointid= {} # point -> pointid

initial_data= []
ctr=0
DS_ctr=0

for i in initial_sample:
	t= i.replace("\n","").split(",")
	item= t[2:]
	initial_data.append(item)
	point_ids[DS_ctr]= t[0]
	pointid_to_point[t[0]]= item
	point_to_pointid[str(item)]= t[0]
	ctr= ctr+1
	DS_ctr= DS_ctr+1
d= len(initial_data[0])
threshold_dist= 2*np.sqrt(d)

# print(str(initial_data[0]))
# print(str(len(initial_sample)))
# print(str(len(initial_data)))
# print(str(lines))

X= np.array(initial_data)
kmeans = KMeans(n_clusters=10*num_clusters, random_state=0)
# kmeans = KMeans(n_clusters=num_clusters, random_state=0)
# kmeans.labels_
clusters1= kmeans.fit_predict(X)
cluster_centers= kmeans.cluster_centers_
# print(str(len(clusters1)))
clusters= {}
ctr=0
for clusterid in clusters1:
	point= initial_data[ctr]
	if(clusterid in clusters):
		clusters[clusterid].append(point)
	else:
		clusters[clusterid]= [point]
	ctr= ctr+1

# print(str(clusters[0]))
# print(str(len(clusters[0])))
# print(cluster_centers)
# print(str(len(cluster_centers)))

DS=[]
CS={}
RS={} # DICT : KEY:POINT_ID VAL:POINT_COORDS

ctr=0
for key in clusters.keys():
	if(len(clusters[key])==1):

		# RS.append(clusters[key][0])
		pos= initial_data.index(clusters[key][0])
		RS[point_ids[pos]]= clusters[key][0]
		initial_data.remove(clusters[key][0])
		for i in range(pos,len(point_ids)-1):
			point_ids[i]= point_ids[i+1]
		ctr= ctr+1


# 	print(len(clusters[key])) 
# print(str(ctr))

# print("Retained Set :")
# for i in RS:
# 	print(str(i))
# print(str(len(RS)))
# print(str(len(initial_data)))

# ============================================= Cluster initially loaded data - RS points to find DS =============================================

X= np.array(initial_data)
kmeans = KMeans(n_clusters=num_clusters, random_state=0)
clusters2= kmeans.fit_predict(X)
cluster_centers= kmeans.cluster_centers_
# print(str(len(clusters1)))
clusters= {}
ctr=0
for clusterid in clusters2:
	point= initial_data[ctr]
	if(clusterid in clusters):
		clusters[clusterid].append(ctr)
	else:
		clusters[clusterid]= [ctr]
	ctr= ctr+1

DS_statistics= {}
for key in clusters.keys():
	DS_statistics[key]= {}

	#Points
	DS_statistics[key][0]= []
	for i in clusters[key]:
		DS_statistics[key][0].append(point_ids[i])
	# N
	DS_statistics[key][1]= len(DS_statistics[key][0]) 
	#SUM
	DS_statistics[key][2]= np.sum(X[clusters[key],:].astype(np.float), axis = 0) 
	#SUMSQ
	DS_statistics[key][3]= np.sum((X[clusters[key],:].astype(np.float))**2, axis = 0) 
	#STDDEV
	DS_statistics[key][4]= np.sqrt((DS_statistics[key][3][:]/DS_statistics[key][1]) - (np.square(DS_statistics[key][2][:])/(DS_statistics[key][1]**2)))
	#CENTROID
	DS_statistics[key][5]= DS_statistics[key][2]/DS_statistics[key][1]

# ================================================== Create CS from the Points in RS ==================================================
RS_points= []  # LIST: POINTS
for key in RS.keys():
	RS_points.append(RS[key])

X= np.array(RS_points)
kmeans = KMeans(n_clusters=int(len(RS_points)/2+1), random_state=0)
clusters3= kmeans.fit_predict(X)
# print(clusters3)

rs_clusters= {}
ctr=0
for clusterid in clusters3:
	if(clusterid in rs_clusters):
		rs_clusters[clusterid].append(ctr)
	else:
		rs_clusters[clusterid]= [ctr]
	ctr= ctr+1

CS_statistics= {}
for key in rs_clusters.keys():
	if(len(rs_clusters[key])>1):
		CS_statistics[key]= {}
		#Points
		CS_statistics[key][0]= []
		for i in rs_clusters[key]:
			pointid= list(RS.keys())[list(RS.values()).index(RS_points[i])]
			CS_statistics[key][0].append(pointid)
				
		# N
		CS_statistics[key][1]= len(rs_clusters[key]) 
		#SUM
		CS_statistics[key][2]= np.sum(X[rs_clusters[key],:].astype(np.float), axis = 0) 
		#SUMSQ
		CS_statistics[key][3]= np.sum((X[rs_clusters[key],:].astype(np.float))**2, axis = 0) 
		#STDDEV
		CS_statistics[key][4]= np.sqrt((CS_statistics[key][3][:]/CS_statistics[key][1]) - (np.square(CS_statistics[key][2][:])/(CS_statistics[key][1]**2)))
		#CENTROID
		CS_statistics[key][5]= CS_statistics[key][2]/CS_statistics[key][1]

for key in rs_clusters.keys():
	if(len(rs_clusters[key])>1):
		
		for i in rs_clusters[key]:
			dict_key_to_remove= list(RS.keys())[list(RS.values()).index(RS_points[i])]
			del RS[dict_key_to_remove]

RS_points= [] 
for key in RS.keys():
	RS_points.append(RS[key])

n_points_DS= 0 
n_clusters_CS= 0
n_points_CS= 0
n_points_RS= 0

for key in DS_statistics.keys():
	n_points_DS+= len(DS_statistics[key][0])

for key in CS_statistics.keys():
	n_clusters_CS+= 1
	n_points_CS+= len(CS_statistics[key][0])

n_points_RS= len(RS_points)

f = open(out_file, "w")
print("Round 1: "+str(n_points_DS)+","+str(n_clusters_CS)+","+str(n_points_CS)+","+str(n_points_RS))
f.write("Round 1: "+str(n_points_DS)+","+str(n_clusters_CS)+","+str(n_points_CS)+","+str(n_points_RS))

# ================================================== Iterate over rest of the data and assign to DS, CS, RS ==================================================

for ite in range(1,5):
	next_sample= []
	if(ite==4):
		next_sample = np.random.choice(a= data, size= lastiter_lines, replace= False)
		# next_sample = np.random.choice(a= data, size= 20, replace= False)
	else:
		next_sample = np.random.choice(a= data, size= lines, replace= False)
		# next_sample = np.random.choice(a= data, size= 20, replace= False)

	next_data= []
	
	index= DS_ctr
	# Point Assignments
	for i in next_sample:
		t= i.replace("\n","").split(",")
		item= t[2:]
		next_data.append(item)
		point_ids[DS_ctr]= t[0]
		pointid_to_point[t[0]]= item
		point_to_pointid[str(item)]= t[0]
		DS_ctr= DS_ctr+1

	# Try to assign to DS
	X= np.array(next_data)
	
	ctr= 0
	for i in X:
		mindist= threshold_dist
		mincluster= -1
		point= i.astype(np.float)
		pointid= point_ids[index+ctr]

		for key in DS_statistics.keys():
			stddev= DS_statistics[key][4].astype(np.float)
			centroid= DS_statistics[key][5].astype(np.float)
			MD= 0
			for dim in range(0,d):
				MD+=((point[dim]-centroid[dim])/stddev[dim])**2
			MD= np.sqrt(MD)

			if(MD<mindist):
				mindist= MD
				mincluster= key

		if(mincluster>-1): 
			#Assign to cluster
			update_DS_statistics(pointid, point, mincluster)

		else:
			# Try to assign to CS
			mindist= threshold_dist
			mincluster= -1

			for key in CS_statistics.keys():
				stddev= CS_statistics[key][4].astype(np.float)
				centroid= CS_statistics[key][5].astype(np.float)
				MD= 0
				for dim in range(0,d):
					MD+=((point[dim]-centroid[dim])/stddev[dim])**2
				MD= np.sqrt(MD)

				if(MD<mindist):
					mindist= MD
					mincluster= key

			if(mincluster>-1): 
				#Assign to cluster
				update_CS_statistics(pointid, point, mincluster)
			else:
				# Assign to RS
				RS[pointid]= list(i)
				RS_points.append(list(i))
		ctr= ctr+1

	# ================================================== Creating CS from RS ==================================================
	X= np.array(RS_points)
	kmeans = KMeans(n_clusters=int(len(RS_points)/2+1), random_state=0)
	clusters4= kmeans.fit_predict(X)
	# print(clusters3)

	rs_clusters= {}
	ctr=0
	for clusterid in clusters4:
		if(clusterid in rs_clusters):
			rs_clusters[clusterid].append(ctr)
		else:
			rs_clusters[clusterid]= [ctr]
		ctr= ctr+1

	# CS_statistics= {}
	for key in rs_clusters.keys():
		if(len(rs_clusters[key])>1):
			k=0
			if(key in CS_statistics.keys()):
				while(k in CS_statistics):
					k=k+1
			else:
				k=key

			CS_statistics[k]= {}
			#Points
			CS_statistics[k][0]= []
			for i in rs_clusters[key]:
				pointid= list(RS.keys())[list(RS.values()).index(RS_points[i])]
				CS_statistics[k][0].append(pointid)
			# N
			CS_statistics[k][1]= len(rs_clusters[key]) 
			#SUM
			CS_statistics[k][2]= np.sum(X[rs_clusters[key],:].astype(np.float), axis = 0) 
			#SUMSQ
			CS_statistics[k][3]= np.sum((X[rs_clusters[key],:].astype(np.float))**2, axis = 0) 
			#STDDEV
			CS_statistics[k][4]= np.sqrt((CS_statistics[k][3][:]/CS_statistics[k][1]) - (np.square(CS_statistics[k][2][:])/(CS_statistics[k][1]**2)))
			#CENTROID
			CS_statistics[k][5]= CS_statistics[k][2]/CS_statistics[k][1]

	for key in rs_clusters.keys():
		if(len(rs_clusters[key])>1):
			for i in rs_clusters[key]:
				# dict_key_to_remove= list(RS.keys())[list(RS.values()).index(RS_points[i])]
				dict_key_to_remove= point_to_pointid[str(RS_points[i])]
				if(dict_key_to_remove in RS.keys()):
					del RS[dict_key_to_remove]

	RS_points= [] 
	for key in RS.keys():
		RS_points.append(RS[key])

	# ================================================== Merge Close CS ==================================================

	list_cs_keys= CS_statistics.keys()
	closest= {}
	for x in list_cs_keys :
		min_MD= threshold_dist
		min_cluster= x
		for y in list_cs_keys :
			if(x!=y):
				stddev1= CS_statistics[x][4]
				stddev2= CS_statistics[y][4]

				centroid1= CS_statistics[x][5]
				centroid2= CS_statistics[y][5]

				MD1= 0
				MD2= 0
				for dim in range(0,d):
					MD1+=((centroid1[dim]-centroid2[dim])/stddev2[dim])**2
					MD2+=((centroid2[dim]-centroid1[dim])/stddev1[dim])**2
				MD1= np.sqrt(MD1)
				MD2= np.sqrt(MD2)

				MD= min(MD1,MD2)
				if(MD<min_MD):
					min_MD= MD
					min_cluster= y

		closest[x]= min_cluster

	for i in closest.keys():
		if(i!=closest[i] and closest[i] in CS_statistics.keys() and i in CS_statistics.keys()):
			merge_CS_clusters(i,closest[i])
			del CS_statistics[closest[i]]
		# print(str(i)+" : "+str(closest[i]))

	# ================================================== Merge CS to Closest DS ==================================================
	if(ite==4):
		list_cs_keys= CS_statistics.keys()
		list_ds_keys= DS_statistics.keys()
		closest= {}

		for x in list_cs_keys :
			min_MD= threshold_dist
			min_cluster= -30
			for y in list_ds_keys :
				if(x!=y):
					stddev1= CS_statistics[x][4]
					stddev2= DS_statistics[y][4]

					centroid1= CS_statistics[x][5]
					centroid2= DS_statistics[y][5]

					MD1= 0
					MD2= 0
					for dim in range(0,d):
						MD1+=((centroid1[dim]-centroid2[dim])/stddev2[dim])**2
						MD2+=((centroid2[dim]-centroid1[dim])/stddev1[dim])**2
					MD1= np.sqrt(MD1)
					MD2= np.sqrt(MD2)

					MD= min(MD1,MD2)
					if(MD<min_MD):
						min_MD= MD
						min_cluster= y

			closest[x]= min_cluster
			# if(min_cluster!=x):
			# 	list_discarded.append(y)
			# 	merge_CS_clusters(x,y)
		# print(list_cs_keys)
		for i in closest.keys():
			if(closest[i] in DS_statistics.keys() and i in CS_statistics.keys()):
				merge_with_DS_cluster(i,closest[i])
				del CS_statistics[i]
			# print(str(i)+" : "+str(closest[i]))

	n_points_DS= 0 
	n_clusters_CS= 0
	n_points_CS= 0
	n_points_RS= 0

	for key in DS_statistics.keys():
		n_points_DS+= len(DS_statistics[key][0])

	for key in CS_statistics.keys():
		n_clusters_CS+= 1
		n_points_CS+= len(CS_statistics[key][0])

	n_points_RS= len(RS_points)

	# print("After Merging CS and DS")

	print("Round "+str(ite+1)+": "+str(n_points_DS)+","+str(n_clusters_CS)+","+str(n_points_CS)+","+str(n_points_RS))
	f.write("\nRound "+str(ite+1)+": "+str(n_points_DS)+","+str(n_clusters_CS)+","+str(n_points_CS)+","+str(n_points_RS))

	break

f.write("\n")
clusterlist= {}
for key in DS_statistics:
	for point in DS_statistics[key][0]:
		clusterlist[point]= key
for key in CS_statistics:
	for point in CS_statistics[key][0]:
		clusterlist[point]=-1
for key in RS:
	clusterlist[key]=-1

for key in sorted(clusterlist.keys(), key=int):
	f.write("\n"+str(key)+","+str(clusterlist[key]))

print(len(clusterlist))
f.close()

end= time.time()
print("Duration: "+str(end-start))