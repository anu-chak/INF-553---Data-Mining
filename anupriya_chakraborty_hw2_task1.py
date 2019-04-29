import sys
from pyspark import SparkConf, SparkContext
import json
import time
import itertools

start= time.time()
sc = SparkContext()

case_no = int(sys.argv[1])
support = int(sys.argv[2])
data_file = sys.argv[3]
out_file = sys.argv[4]

# ==================================================== FUNCTION : CREATE BASKETS  =============================================+======
def create_baskets(rdd) :
	if case_no==1:
		res= rdd.map(lambda a: (a[0], [a[1]])).reduceByKey(lambda a,b: a+b).persist().filter(lambda x: x[0]!= "user_id")
	else :
		res= rdd.map(lambda a:(a[1], a[0])).map(lambda a: (a[0], [a[1]])).reduceByKey(lambda a,b: a+b).persist().filter(lambda x: x[0]!= "business_id")
	return res

# ============================================== FUNCTION : COUNT FREQUENCY OF ITEMSET  ==============================================
def count_frequency(itemset, superset) :

	count=0
	for l in superset :
		if(set(itemset).issubset(l)) :
			count= count+1
	return count

def count_frequency_SONPhase2(baskets_list, itemsets) :

	baskets= list(baskets_list)
	# print(baskets)
	# print(str(itemsets))
	frequencies= list()

	for i in itemsets :
		count=0
		for b in baskets :
			if (set(i).issubset(b)) :
				count= count+1
		frequencies.append([i,count])
			# elif (set([i]).issubset(b)) :
			# 	count= count+1
			# 	frequencies.append([(i,),count])
	return frequencies


# ============================================== FUNCTION : GENERATE CANDIDATE ITEMSETS  =============================================

def generateCandidateItemsets(frequent_itemsets, k):

	candidates= list()

	for i in range(len(frequent_itemsets)-1) :
		for j in range(i+1, len(frequent_itemsets)) :
			if(frequent_itemsets[i][0:k-2] == frequent_itemsets[j][0:k-2]) :
				candidate = list(set(frequent_itemsets[i])|set(frequent_itemsets[j]))
				candidate.sort()
				if(candidate not in candidates) : 
					candidates.append(candidate)
			else :
				break
	return candidates

# ================================================== FUNCTION : A PRIORI ALGORITHM  ==================================================

def apriori(baskets_list, itemset, support_threshold) :
	
	baskets= list(baskets_list)
	size= 1;

	frequent_itemsets= list()
	C= []
	L= []

	k=1

	for i in itemset :
		count=0
		for b in baskets :
			if (i in b) :
				count= count+1

		C.append(i)
		if(count >= support_threshold) :
			L.append(i)

	C.sort()
	L.sort()
	length_l1= len(L) 

	# print(str(L))
	L1= [(x,) for x in L]
	frequent_itemsets.extend(L1)

	# for l in L1 :
	# 	print(str(l))
	k= k+1

	# ------------------- Generating pairs -------------------
	C = list()
	pair= []
	for x in itertools.combinations(L, 2) :
		pair= list(x)
		pair.sort()
		C.append(pair)
	C.sort()

	L.clear()
	for c in C :
		count= count_frequency(c,baskets) 
		if (count>=support_threshold) :
			L.append(c)

	L.sort()
	# for l in L :
	# 	print(str(l))
	frequent_itemsets.extend(L)
	k= k+1

	# ------------------ Start of for loop -------------------
	while k!=length_l1 :	

		C.clear()
		C= generateCandidateItemsets(L, k)
		# print(str(C))
		if(len(C)==0) :
			break
		C.sort()

		L.clear()
		for c in C :
			count= count_frequency(c, baskets)
			if(count >= support_threshold) :
				L.append(c)
		L.sort()
		# for l in L :
		# 	print(str(l))
		frequent_itemsets.extend(L)
		k= k+1

	# Displaying Frequent Itemsets
	# for t in frequent_itemsets :
	# 	print(str(t))

	return frequent_itemsets

# ======================================================= DRIVER PROGRAM START  ======================================================
read_data = sc.textFile(data_file)
rdd= read_data.map(lambda x : x.split(',')).map(lambda a:(a[0], a[1])).distinct()
baskets= create_baskets(rdd).map(lambda a: a[1])
baskets_list= baskets.collect()

# for b in baskets_list :
# 	print(str(b))

list_of_items_in_baskets= baskets.collect()
list_of_items= list(set().union(*list_of_items_in_baskets))
# rdd_of_items= sc.parallelize(list(set().union(*list_of_items_in_baskets)))
# apriori(baskets_list, list_of_items, support)

# ===================================================== FUNCTION : SON ALGORITHM  ====================================================
num_partitions= read_data.getNumPartitions()
support_threshold= support/num_partitions

# -------------------------------------------------------------- PHASE 1 -------------------------------------------------------------
SONPhase1Map= baskets.mapPartitions(lambda b: apriori(b, list_of_items, support_threshold)).map(lambda x: (tuple(x),1))
SONPhase1Reduce= SONPhase1Map.distinct().sortByKey().map(lambda a: a[0]).collect()
# for x in SONPhase1Reduce :
# 	print(str(x))

#-------------------------------------------------------------- PHASE 2 -------------------------------------------------------------
SONPhase2Map= baskets.mapPartitions(lambda b: count_frequency_SONPhase2(b, SONPhase1Reduce))
SONPhase2Reduce= SONPhase2Map.reduceByKey(lambda a,b: (a+b)).filter(lambda a: a[1]>=support).sortByKey().collect()
# # print(str(SONPhase2Reduce))
# for x in SONPhase2Reduce :
# 	print(str(x))

# #====================================================== WRITING TO OUTPUT FILE ======================================================
f = open(out_file, 'w')

# ------------------------------------------------------------- CANDIDATES -----------------------------------------------------------
f.write("Candidates:")
f.write("\n")
l= 1
while l != len(list_of_items) :
	s=""
	for r in SONPhase1Reduce :
		if (len(r)==l) :
			s= s+str(r)
	# print(s)
	
	s= s.replace(",)",")").replace(")(","),(")
	if(s=="") :
		break
	else :
		if(l!=1) :
			f.write("\n\n")	
		f.write(s)
	l= l+1
f.write("\n\n")

# ------------------------------------------------------------- FREQUENT -------------------------------------------------------------
f.write("Frequent Itemsets:")
f.write("\n")
l= 1
while l != len(list_of_items) :
	s=""
	for b in SONPhase2Reduce :
		if (len(b[0])==l) :
			s= s+str(b[0])
	s= s.replace(",)",")").replace(")(","),(")
	if(s=="") :
		break
	else :
		if(l!=1) :
			f.write("\n\n")	
		f.write(s)
	l= l+1
f.close()

end= time.time()
print("Duration: "+str(end-start))