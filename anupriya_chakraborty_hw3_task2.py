from __future__ import print_function
import sys
from pyspark import SparkConf, SparkContext
import time
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import math
import itertools

start= time.time()
cosine_similarities_matrix= {}
a= [1, 3, 9, 11, 13, 17, 19, 27, 29, 31, 33, 37, 39, 41, 43, 47, 51, 53, 57, 59]
m= 0

num_of_hash_functions= 20
index1= -1
row_per_band= 2
# ================================================ MODEL BASED COLLABORATIVE FILTERING ===============================================
def modelbased_cf_recommendation(train_rdd, test_rdd):

	full_rdd= sc.union([train_rdd, test_rdd])

	users_rdd= full_rdd.map(lambda a:a[0]).distinct()
	businesses_rdd= full_rdd.map(lambda a:a[1]).distinct()

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

	# Build the recommendation model using Alternating Least Squares
	ratings = train_rdd.map(lambda x: Rating(int(users_dict[x[0]]), int(businesses_dict[x[1]]), float(x[2])))

	rank = 3
	numIterations = 10
	model = ALS.train(ratings, rank, numIterations, 0.1)

    # Evaluate the model on testing data
	testing_data= test_rdd.map(lambda x: Rating(int(users_dict[x[0]]), int(businesses_dict[x[1]]), float(x[2])))
	testdata = testing_data.map(lambda x: (x[0], x[1]))
	predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))

	ratesAndPreds = testing_data.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
	# MSE = ratesAndPreds.map(lambda r: ((r[1][0] - r[1][1])*(r[1][0] - r[1][1]))).mean()
	# print("Root Mean Squared Error = " + str(math.sqrt(MSE)))

# ====================================================== WRITING TO OUTPUT FILE ======================================================	
	f = open(out_file, 'w')

	f.write("user_id, business_id, prediction")
	for i in ratesAndPreds.collect():
		f.write("\n")
		f.write(users[i[0][0]]+","+businesses[i[0][1]]+","+str(i[1][1]))
	f.close()





# ================================================ USER BASED COLLABORATIVE FILTERING ================================================

def get_normalised_ratings(x):
	user= x[0]
	ratings= x[1]

	avg_rating=0

	for i in ratings:
		avg_rating= avg_rating+float(i)
	avg_rating= avg_rating/len(ratings)

	new_ratings=[]
	for i in ratings:
		new_ratings.append(float(i))
		# new_ratings.append(float(i)-avg_rating)
	return (user,(new_ratings,avg_rating))

def convert(x):
	user_id= x[0][0]
	businesses= x[0][1]
	ratings= x[1][0]
	avg_rating= x[1][1]

	rows= []
	for i in range(0,len(businesses)):
		row= ((user_id, businesses[i]),(ratings[i],avg_rating))
		rows.append(row)
	return rows

def get_user_rating(x, keyed_ratings, list_userwise_businesses, businesswise_users, N):

	user_id1= x[0]
	business_id1= x[1]
	businesses_rated= list_userwise_businesses[user_id1]

	ctr1=0
	avg_rating1= keyed_ratings[(user_id1,list_userwise_businesses[user_id1][0])][1]

	# # -------------------------------- Calculating Cosine Similarity --------------------------------

	numerator=0
	denominator1=1
	denominator2=1

	if(business_id1 not in businesswise_users.keys()) :
		return ((x[0],x[1]), 0)
	else : 	
		co_rated_user_ids= businesswise_users[business_id1]
		cosine_similarities= []
		
		for i in co_rated_user_ids:
			businesses_rated_by_co_rater= list_userwise_businesses[i]
			co_rated_businesses= set(businesses_rated) & set(businesses_rated_by_co_rater)
			
			ctr=0
			numerator=0
			denominator1=0
			denominator2=0
			cosine_similarity=0

			avg_rating2= keyed_ratings[(i,business_id1)][1]

			for j in co_rated_businesses:
				if(j!=business_id1 and ctr<N):
					rating1= float(keyed_ratings[(user_id1,j)][0])-avg_rating1
					rating2= float(keyed_ratings[(i,j)][0])-avg_rating2

					numerator= numerator + rating1*rating2
					denominator1= denominator1 + rating1*rating1
					denominator2= denominator2 + rating2*rating2
					ctr= ctr+1
				else:
					pass
			if(numerator!=0):
				cosine_similarity= float(numerator/math.sqrt(denominator1*denominator2))
			cosine_similarities.append((i,cosine_similarity,avg_rating2))
		
		cosine_similarities.sort(key=lambda x: x[1], reverse=True)

		# --- ----------------------------- Calculating Weighted Average --------------------------------	

		weighted_average=0
		num=0
		den=0
		count=0

		for i in cosine_similarities:
			if(count<N):
				co_rater= i[0]
				cosine_similarity= i[1]
				avg_rating= i[2]

				co_rating= float(keyed_ratings[(co_rater,business_id1)][0])-avg_rating

				num= num + co_rating*cosine_similarity
				den= den + abs(cosine_similarity)
				count= count+1
			else:
				break

		if(den==0): 
			return ((x[0],x[1]),0)
		else :
			rating= float(avg_rating1) + (num/den)
			score= ((x[0],x[1]),rating)
			return score

def userbased_cf_recommendation(train_rdd, test_rdd):

	userwise_businesses= train_rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y)
	userwise_ratings= train_rdd.map(lambda x: (x[0], [x[2]])).reduceByKey(lambda x,y: x+y)
	normalized= userwise_ratings.map(lambda x: get_normalised_ratings(x))

	userwise_business_ratings= userwise_businesses.join(normalized).map(lambda x: ((x[0],x[1][0]),(x[1][1][0],x[1][1][1])))

	list_userwise_businesses= userwise_businesses.collectAsMap()

	keyed_ratings= userwise_business_ratings.flatMap(lambda x: convert(x)).collectAsMap()

	businesswise_users= train_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y).collectAsMap()
	test_pairs= test_rdd.map(lambda x: (x[0],x[1]))
	

	global N
	predicted_ratings= test_pairs.map(lambda x: get_user_rating(x, keyed_ratings, list_userwise_businesses, businesswise_users, N))

	# ratesAndPreds = test_rdd.map(lambda x: ((x[0], x[1]), float(x[2]))).join(predicted_ratings)
	# MSE = ratesAndPreds.map(lambda r: ((r[1][0] - r[1][1])*(r[1][0] - r[1][1]))).mean()
	# print("Root Mean Squared Error = " + str(math.sqrt(MSE)))

# ====================================================== WRITING TO OUTPUT FILE ======================================================	
	f = open(out_file, 'w')

	f.write("user_id, business_id, prediction")
	for i in predicted_ratings.collect():
		f.write("\n")
		f.write(i[0][0]+","+i[0][1]+","+str(i[1]))
	f.close()




# ================================================ ITEM BASED COLLABORATIVE FILTERING ================================================
def convert2(x):
	business_id= x[0][0]
	users= x[0][1]
	ratings= x[1][0]
	avg_rating= x[1][1]

	rows= []
	for i in range(0,len(users)):
		row= ((users[i], business_id),(ratings[i],avg_rating))
		rows.append(row)
	return rows

def get_cosine_similarities(x, list_businesseswise_user_ratings, keyed_ratings):

	business1= x[0]
	business2= x[1]
	# print(business1)
	# print(business2)

	if(business1 in list_businesseswise_user_ratings and business2 in list_businesseswise_user_ratings):
		userlist1= list_businesseswise_user_ratings[business1][0]
		userlist2= list_businesseswise_user_ratings[business2][0]

		ratinglist1= list_businesseswise_user_ratings[business1][1]
		ratinglist2= list_businesseswise_user_ratings[business2][1]
		# print(str(userlist1))
		# print(str(userlist2))

		avg_rating1= list_businesseswise_user_ratings[business1][2]
		avg_rating2= list_businesseswise_user_ratings[business2][2]

		co_rated_users= set(userlist1) & set(userlist2)

		global N2
		count=0
		numerator=0
		denominator1=0
		denominator2=0
		cosine_similarity=0		
		for j in co_rated_users:
			if(count<N2):
				rating1= float(keyed_ratings[(j,business1)][0])
				# -avg_rating1
				rating2= float(keyed_ratings[(j,business2)][0])
				# -avg_rating2
				# print("rating1: "+str(rating1))
				# print("rating2: "+str(rating2))

				numerator= numerator + rating1*rating2
				denominator1= denominator1 + rating1*rating1
				denominator2= denominator2 + rating2*rating2
				count= count+1
			else:
				break

		if(numerator!=0):
			# print("Numerator: "+str(numerator))
			# print("denominator1: "+str(denominator1))
			# print("denominator2: "+str(denominator2))
			cosine_similarity= float(numerator/math.sqrt(denominator1*denominator2))
			# print("cosine_similarity: "+str(cosine_similarity))
			# print("\n")

		global cosine_similarities_matrix
		cosine_similarities_matrix[x]= cosine_similarity

		return cosine_similarity
	else:
		return 0
		

def get_item_rating(x, userwise_businesses, list_businesseswise_user_ratings, keyed_ratings):

	user_id= x[0]
	business_id= x[1]

	#-------------------------------- Calculating Weighted Average --------------------------------
	weighted_average=0
	num=0
	den=0

	businesses= userwise_businesses[user_id]
	# if(business_id in list_businesseswise_user_ratings):
	# 	users_rated= list_businesseswise_user_ratings[business_id][0]
	# 	for u in users_rated:
	# 		businesses.extend(userwise_businesses[u])
	# 	businesses= list(set(businesses))
	count=0
	cosine_similarities=[]
	backup=[]
	for b in businesses:
		cosine_similarity= 0
		if(b!=business_id):
			if((business_id,b) in cosine_similarities_matrix): 
				cosine_similarity= cosine_similarities_matrix[(business_id,b)]
				# print("A")
			elif ((b,business_id) in cosine_similarities_matrix):
				cosine_similarity= cosine_similarities_matrix[(b,business_id)]
				# print("B")
			elif(business_id in list_businesseswise_user_ratings and b in list_businesseswise_user_ratings):
				cosine_similarity= get_cosine_similarities((business_id,b), list_businesseswise_user_ratings, keyed_ratings)
				# print("C")
			else :
				cosine_similarity= 0

			backup.append((b,cosine_similarity))
			if(cosine_similarity>=1):
				cosine_similarities.append((b,cosine_similarity))
				count= count+1

			if(count>=N2):
				break

	if(len(backup)<N2):
		cosine_similarities= backup


		# cosine_similarities.sort(key=lambda x: x[1], reverse=True)

	# global N2
	# cosine_similarities= cosine_similarities[:N2]
	

	num= 0
	den= 0
	for i in cosine_similarities:
		business_id2= i[0]
		cosine_similarity= i[1]

		if((user_id,business_id2) in keyed_ratings):
			avg_rating= float(keyed_ratings[(user_id,business_id2)][1])
			# print(business_id2+" : "+str(cosine_similarity)+" : "+str(avg_rating))

			rating= float(keyed_ratings[(user_id,business_id2)][0])
			# -avg_rating

			# print(str(rating)+", "+str(cosine_similarity))
			num= num + float(rating*cosine_similarity)
			den= den + float(abs(cosine_similarity))

	if(den==0): 
		return ((x[0],x[1]),0)
	else :

		# print("RATING: "+str(num)+"/"+str(den))
		rt= num/den
		# print("Predicted score :"+str(rt))

		score= ((x[0],x[1]),rt)
		return score


def itembased_cf_recommendation(train_rdd, test_rdd):

	businesses= train_rdd.map(lambda x: x[1]).distinct().collect()

	# list_businesseswise_users= businesswise_users.collectAsMap()
	userwise_businesses= train_rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y).collectAsMap()
	businesswise_users= train_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)
	businesswise_ratings= train_rdd.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x,y: x+y)
	normalized= businesswise_ratings.map(lambda x: get_normalised_ratings(x))

	businesswise_user_ratings= businesswise_users.join(normalized).map(lambda x: ((x[0],x[1][0]),(x[1][1][0],x[1][1][1])))
	keyed_ratings= businesswise_user_ratings.flatMap(lambda x: convert2(x)).collectAsMap()
	list_businesseswise_user_ratings= businesswise_user_ratings.map(lambda x : (x[0][0], (x[0][1], x[1][0], x[1][1]))).collectAsMap()

	test_pairs= test_rdd.map(lambda x: (x[0],x[1]))
	

	predicted_ratings= test_pairs.map(lambda x: get_item_rating(x, userwise_businesses, list_businesseswise_user_ratings, keyed_ratings))
	# for i in predicted_ratings.collect():
	# 	print(str(i))

	# ratesAndPreds = test_rdd.map(lambda x: ((x[0], x[1]), float(x[2]))).join(predicted_ratings)
	# MSE = ratesAndPreds.map(lambda r: ((r[1][0] - r[1][1])*(r[1][0] - r[1][1]))).mean()
	# print("Root Mean Squared Error = " + str(math.sqrt(MSE)))

	
# ====================================================== WRITING TO OUTPUT FILE ======================================================	
	f = open(out_file, 'w')

	f.write("user_id, business_id, prediction")
	for i in predicted_ratings.collect():
		f.write("\n")
		f.write(i[0][0]+","+i[0][1]+","+str(i[1]))
	f.close()




# ============================================ IMPROVED ITEM BASED COLLABORATIVE FILTERING ===========================================

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

def find_jaccard_similarity(x, businesswise_users):
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

def get_similar_businesses_lsh(train_rdd):

	# read_data= sc.textFile(data_file)
	# rdd_data= read_data.map(lambda x : x.split(','))
	# rdd= rdd_data.filter(lambda x: x[0]!= "user_id").persist()

	# ----------------------------------------------- Creating Characteristic (0/1) Matrix -----------------------------------------------
	users_rdd= train_rdd.map(lambda a:a[0]).distinct()
	businesses_rdd= train_rdd.map(lambda a:a[1]).distinct()

	businesswise_users= train_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y).collectAsMap()
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

	characteristic_matrix= train_rdd.map(lambda x: (x[1],[users_dict[x[0]]])).reduceByKey(lambda x,y: x+y)
	# print(characteristic_matrix.take(5))

	# # ------------------------------------------------- Generating Minhash Signatures -------------------------------------------------
	global m
	m= nrows
	# num_of_hash_functions= 20
	signature_matrix= characteristic_matrix.map(lambda x: minhashing(x))

	# print(signature_matrix.take(5))


	# --------------------------------------------------- Locality Sensitive Hashing --------------------------------------------------


	sig= signature_matrix.flatMap(lambda x: intermediate_step1(x))

	candidate_gen= sig.reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>1)
	candidates= candidate_gen.flatMap(lambda x: get_candidates(x)).distinct()

	jaccard_similarity_rdd= candidates.map(lambda x: find_jaccard_similarity(x, businesswise_users)).filter(lambda x: x[1]>=0.5)
	return jaccard_similarity_rdd
	# sorted_js_rdd= jaccard_similarity_rdd.map(lambda x: (x[0][1], (x[0][0], x[1]))).sortByKey().map(lambda x: (x[1][0], (x[0],x[1][1])))

def get_improved_item_rating(x, cosines_matrix, userwise_businesses, list_businesseswise_user_ratings, keyed_ratings):

	user_id= x[0]
	business_id= x[1]

	#-------------------------------- Calculating Weighted Average --------------------------------
	weighted_average=0
	num=0
	den=0

	businesses= userwise_businesses[user_id]
	if(business_id in cosines_matrix):
		jaccard= cosines_matrix[business_id]
		businesses= list(set(businesses) & set(jaccard))
	# if(business_id in list_businesseswise_user_ratings):
	# 	users_rated= list_businesseswise_user_ratings[business_id][0]
	# 	for u in users_rated:
	# 		businesses.extend(userwise_businesses[u])
	# 	businesses= list(set(businesses))
	global N3

	count=0
	cosine_similarities=[]
	backup=[]
	for b in businesses:
		cosine_similarity= 0
		if(b!=business_id):


			if((business_id,b) in cosine_similarities_matrix): 
				cosine_similarity= cosine_similarities_matrix[(business_id,b)]
				# print("A")
			elif ((b,business_id) in cosine_similarities_matrix):
				cosine_similarity= cosine_similarities_matrix[(b,business_id)]
				# print("B")
			elif(business_id in list_businesseswise_user_ratings and b in list_businesseswise_user_ratings):
				cosine_similarity= get_cosine_similarities((business_id,b), list_businesseswise_user_ratings, keyed_ratings)
				# print("C")
			else :
				cosine_similarity= 0

			backup.append((b,cosine_similarity))
			if(cosine_similarity>=1):
				cosine_similarities.append((b,cosine_similarity))
				count= count+1

			if(count>=N3):
				break

	if(len(backup)<N3):
		cosine_similarities= backup


		# cosine_similarities.sort(key=lambda x: x[1], reverse=True)

	# global N2
	# cosine_similarities= cosine_similarities[:N2]
	

	num= 0
	den= 0
	for i in cosine_similarities:
		business_id2= i[0]
		cosine_similarity= i[1]

		if((user_id,business_id2) in keyed_ratings):
			avg_rating= float(keyed_ratings[(user_id,business_id2)][1])
			# print(business_id2+" : "+str(cosine_similarity)+" : "+str(avg_rating))

			rating= float(keyed_ratings[(user_id,business_id2)][0])
			# -avg_rating

			# print(str(rating)+", "+str(cosine_similarity))
			num= num + float(rating*cosine_similarity)
			den= den + float(abs(cosine_similarity))

	if(den==0): 
		return ((x[0],x[1]),0)
	else :

		# print("RATING: "+str(num)+"/"+str(den))
		rt= num/den
		# print("Predicted score :"+str(rt))

		score= ((x[0],x[1]),rt)
		return score

def itembased_improved_recommendation(train_rdd, test_rdd):

	businesses= train_rdd.map(lambda x: x[1]).distinct().collect()

	# list_businesseswise_users= businesswise_users.collectAsMap()
	userwise_businesses= train_rdd.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda x,y: x+y).collectAsMap()
	businesswise_users= train_rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)
	businesswise_ratings= train_rdd.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda x,y: x+y)
	normalized= businesswise_ratings.map(lambda x: get_normalised_ratings(x))

	businesswise_user_ratings= businesswise_users.join(normalized).map(lambda x: ((x[0],x[1][0]),(x[1][1][0],x[1][1][1])))
	keyed_ratings= businesswise_user_ratings.flatMap(lambda x: convert2(x)).collectAsMap()
	list_businesseswise_user_ratings= businesswise_user_ratings.map(lambda x : (x[0][0], (x[0][1], x[1][0], x[1][1]))).collectAsMap()

	test_pairs= test_rdd.map(lambda x: (x[0],x[1]))
	

	similar_businesses= get_similar_businesses_lsh(train_rdd).distinct()
	cosines_matrix= similar_businesses.flatMap(lambda x: (((x[0][0],x[0][1]),x[1]),((x[0][1],x[0][0]),x[1]))).map(lambda x: (x[0],[x[1]])).reduceByKey(lambda x,y: x+y).collectAsMap()
		
	# for i in cosines_matrix.take(10):
	# 	print(str(i))
	predicted_ratings= test_pairs.map(lambda x: get_improved_item_rating(x, cosines_matrix, userwise_businesses, list_businesseswise_user_ratings, keyed_ratings))
	# for i in predicted_ratings.collect():
	# 	print(str(i))

	# ratesAndPreds = test_rdd.map(lambda x: ((x[0], x[1]), float(x[2]))).join(predicted_ratings)
	# MSE = ratesAndPreds.map(lambda r: ((r[1][0] - r[1][1])*(r[1][0] - r[1][1]))).mean()
	# print("Root Mean Squared Error = " + str(math.sqrt(MSE)))


# ====================================================== WRITING TO OUTPUT FILE ======================================================	
	f = open(out_file, 'w')

	f.write("user_id, business_id, prediction")
	for i in predicted_ratings.collect():
		f.write("\n")
		f.write(i[0][0]+","+i[0][1]+","+str(i[1]))
	f.close()






# ====================================================== MAIN DRIVER PROGRAM =========================================================	
train_file = sys.argv[1]
test_file = sys.argv[2]
case_id= int(sys.argv[3])
out_file= sys.argv[4]

sc = SparkContext(appName="PythonCollaborativeFilteringExample")

# Load and parse the data
train_data= sc.textFile(train_file)
train_data= train_data.map(lambda x : x.split(','))
train_rdd= train_data.filter(lambda x: x[0]!= "user_id").persist()

test_data= sc.textFile(test_file)
test_data= test_data.map(lambda x : x.split(','))
test_rdd= test_data.filter(lambda x: x[0]!= "user_id").persist()

N=14
N2=4000
N3=4000

if(case_id==1):
	modelbased_cf_recommendation(train_rdd, test_rdd)
elif(case_id==2):
	userbased_cf_recommendation(train_rdd, test_rdd)
elif(case_id==3):
	itembased_cf_recommendation(train_rdd, test_rdd)
elif(case_id==4):
	itembased_improved_recommendation(train_rdd, test_rdd)
else :
	pass
# ----------------------------------------------- PROCESSING TRAINING DATA -----------------------------------------------
end= time.time()
print("Duration: "+str(end-start))