import sys
from pyspark import SparkConf, SparkContext
from collections import defaultdict
import time
import random

start= time.time()
sc = SparkContext()

data_file = sys.argv[1]
out_file = sys.argv[2]

index=-1
index1=-1
index2=-1
row_per_band= 2


# # ======================================== FUNCTION : GENERATE COEFFICIENTS FOR HASH FUNCTION  ========================================

# list.
def find_next_prime(a) :
	for p in range(a, 2*a):
		x=0
		for i in range(2, p):
			x=i
			if p%i == 0:
				break
		if x==p-1 :
			return p
			break

def generate_random_coeffs(k, nrows) :
	# Create a list of 'k' random values.
	coeffsList= list()
	while k > 0:
		# Get a random row.
		randRow= random.randint(0, nrows)
		# Ensure that each random number is unique.
		while randRow in coeffsList :
			randRow= random.randint(0, nrows)
		# Add the random number to the list.
		coeffsList.append(randRow)
		k= k-1
    
	# print(str(coeffsList))
	return coeffsList

def minhashing(x):
	global a
	# global b
	global m
	minNum = [min((ax*x + 1) % m for x in x[1]) for ax in a]
	return (x[0], minNum)

def get_business_signatures(x):
	global index
	index= index+1
	business_id= businesses[index]
	return tuple((business_id, x))

def intermediate_step1(x):
	global index1
	global row_per_band
	bands= int(len(x[1])/row_per_band)

	index1= index1+1
	business_id= x[0]
	signatures_list= x[1]
	# print(str(len(signatures_list)))

	bands_list= []
	rowindex=0
	for b in range(0,bands):
		row= []
		for r in range(0,row_per_band):
			# print(str(rowindex))
			row.append(signatures_list[rowindex])
			rowindex= rowindex+1
		# print(str(row))
		bands_list.append(((b,tuple(row)),[business_id]))
		row.clear()

	return bands_list

def get_candidates(x):
	businesses= x[1]
	businesses.sort()
	candidates= []
	# print(str(businesses))
	for i in range(0,len(businesses)):
		for j in range(i+1, len(businesses)):
			if(j>i):
				candidates.append(((businesses[i], businesses[j]),1))

	return candidates

def find_jaccard_similarity(x):
	business1= x[0][0]
	business2= x[0][1]
	# print(business1+","+business2)
	# return 1
	users1= set(businesswise_users[business1])
	users2= set(businesswise_users[business2])
	
	js= float(len(users1.intersection(users2)) / len(users1.union(users2)))

	return (((business1, business2), js))

def index_businesses(x):
	global index2
	index2= index2+1
	return((x[0], index2))

# ======================================================= DRIVER PROGRAM START  ======================================================
read_data= sc.textFile(data_file)
rdd_data= read_data.map(lambda x : x.split(','))
rdd= rdd_data.filter(lambda x: x[0]!= "user_id").persist()

# ----------------------------------------------- Creating Characteristic (0/1) Matrix -----------------------------------------------
users_rdd= rdd.map(lambda a:a[0]).distinct()
businesses_rdd= rdd.map(lambda a:a[1]).distinct()

businesswise_users= rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y).collectAsMap()
# businesswise_indexes= businesswise_users.map(lambda x: index_businesses(x)).collectAsMap()

users= users_rdd.collect()
businesses= businesses_rdd.collect()

nrows= len(users)
ncols= len(businesses)

users_dict={}
for u in range(0, nrows):
	users_dict[users[u]]= u

businesses_dict={}
for b in range(0, ncols):
	businesses_dict[businesses[b]]= b

characteristic_matrix= rdd.map(lambda x: (x[1],[users_dict[x[0]]])).reduceByKey(lambda x,y: x+y)
# print(characteristic_matrix.take(5))

# # ------------------------------------------------- Generating Minhash Signatures -------------------------------------------------
a= [1, 3, 9, 11, 13, 17, 19, 27, 29, 31, 33, 37, 39, 41, 43, 47, 51, 53, 57, 59]
m= nrows
num_of_hash_functions= 20
signature_matrix= characteristic_matrix.map(lambda x: minhashing(x))

print(signature_matrix.take(5))


# --------------------------------------------------- Locality Sensitive Hashing --------------------------------------------------


sig= signature_matrix.flatMap(lambda x: intermediate_step1(x))

candidate_gen= sig.reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>1)
candidates= candidate_gen.flatMap(lambda x: get_candidates(x)).distinct()

jaccard_similarity_rdd= candidates.map(lambda x: find_jaccard_similarity(x)).filter(lambda x: x[1]>=0.5)
sorted_js_rdd= jaccard_similarity_rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).sortByKey().map(lambda x: (x[1][0], (x[0],x[1][1]))).sortByKey()


# ground = sc.textFile("/Users/anupriyachakraborty/Documents/USC-Semester-Work/Sem-4/Data-Mining/Homework/HW3/pure_jaccard_similarity.csv").map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))

# rrr = jaccard_similarity_rdd.map(lambda x: (x[0][0], x[0][1]))
# rrrrr = list(rrr.collect())
# ggggg = list(ground.collect())
# tp = rrr.intersection(ground)
# ttttt= list(tp.collect())
    
# precision = len(ttttt)/len(rrrrr)
# recall = len(ttttt)/len(ggggg)
# print("precision:")
# print(precision)
# print("recall:")
# print(recall)

# #====================================================== WRITING TO OUTPUT FILE ======================================================
f = open(out_file, 'w')

f.write("business_id_1, business_id_2, similarity")
for i in sorted_js_rdd.collect():
	f.write("\n")
	f.write(i[0]+","+i[1][0]+","+str(i[1][1]))
f.close()

end= time.time()

print(str(jaccard_similarity_rdd.count()))
print(str(len(ggggg)))
print("Duration: "+str(end-start))