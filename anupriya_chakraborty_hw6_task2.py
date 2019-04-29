from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import json
import binascii
import random
import math

port_num = int(sys.argv[1])
out_file = sys.argv[2]
f= open(out_file, "w")
f.write("Time,Ground Truth,Estimation")
f.close()

num_hash= 45
num_groups= 9
rows_per_group= int(num_hash/num_groups)

hash_functions= []
for i in range(0,num_hash):
	h= []
	for j in range(0,2):
		r= random.randint(1,100)
		h.append(r)
	hash_functions.append(h)
# for i in hash_functions:
# 	print(i)

m= 450

def trailing_zeroes(mystr):
    return len(mystr)-len(mystr.rstrip('0'))

def flajolet_martin(time, rdd):

	global num_hash
	global num_groups
	global rows_per_group
	global hash_functions
	global m

	print(time)
	data= rdd.collect()
	# print(len(data))
	# print(data)
	actual_distinct_cities= set()

	hashvals= []
	hashvals_bin= []
	for i in data:
		obj= json.loads(i)
		city= obj["city"]
		actual_distinct_cities.add(city)

		int_city= int(binascii.hexlify(city.encode('utf8')),16)

		h1= []
		h2= []
		for i in hash_functions:
			val= (i[0] * int_city + i[1]) % m
			to_bin= bin(val)[2:]			
			# print(to_bin)
			h1.append(val)
			h2.append(to_bin)

		hashvals.append(h1)
		hashvals_bin.append(h2)

	# Citywise Hashvalues received
	# Now get max # zeroes for entire set of cities for each hash function 
	# Hence calculate the estimates for each hash function

	estimates= []
	for i in range(0,num_hash):
		maxval= -1
		for j in range(0,len(hashvals_bin)):
			# print(hashvals_bin[j][i])
			z= trailing_zeroes(hashvals_bin[j][i])
			if(z>maxval):
				maxval= z
		estimates.append(math.pow(2,maxval))

	# for i in estimates:
	# 	print(i)

	# Group the hash functions and get the averages for each group of hash functions
	groupwise_averages= []
	for i in range(0,num_groups):
		avg= 0
		for j in range(0,rows_per_group):
			index= i*rows_per_group + j
			avg+= estimates[index]

		avg= round(avg/rows_per_group)
		groupwise_averages.append(avg)

	groupwise_averages.sort()
	predicted_num_distinct= groupwise_averages[int(num_groups/2)]

	f= open(out_file, "a")
	# f.write("\n"+str(actual_distinct_cities))
	f.write("\n"+str(time)+","+str(len(actual_distinct_cities))+","+str(predicted_num_distinct))
	f.close()


sc = SparkContext("local[2]", "Flajolet_Martin")
ssc = StreamingContext(sc, 5)
current_stream = ssc.socketTextStream("localhost", port_num).window(30,10)
# windowed_stream= current_stream.window(30,10)
current_stream.foreachRDD(flajolet_martin)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
f.close()