from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import json
import binascii
import random

port_num = int(sys.argv[1])
out_file = sys.argv[2]
f= open(out_file, "w")
f.write("Time,FPR")
f.close()

filter_bit_array= []
for i in range(0,200):
	filter_bit_array.append(0)
# print(len(filter_bit_array))

hash_functions= []
for i in range(0,10):
	h= []
	for j in range(0,2):
		r= random.randint(1,100)
		h.append(r)
	hash_functions.append(h)
# for i in hash_functions:
# 	print(i)

m= 200

actual_seen_cities= set()
predicted_seen_cities= set()

false_positive= 0
true_negative= 0

def bloom_filtering(time, rdd):

	global filter_bit_array
	global hash_functions
	global m
	global actual_seen_cities
	global predicted_seen_cities
	global true_positive
	global false_positive
	global true_negative
	global false_negative
	global f

	print(time)
	data= rdd.collect()

	for i in data:
		obj= json.loads(i)
		city= obj["city"]

		int_city= int(binascii.hexlify(city.encode('utf8')),16)

		hashvals= []
		for i in hash_functions:
			hashvals.append((i[0] * int_city + i[1]) % m)

		ctr= 0
		for i in hashvals:
			if(filter_bit_array[i]==1):
				ctr+= 1

		if(city not in actual_seen_cities):
			if(ctr==len(hashvals)):
				false_positive+= 1
				predicted_seen_cities.add(city)
			else:
				true_negative+= 1

		# Update the Bloom Filter
		for i in hashvals:
			if(filter_bit_array[i]!=1):
				filter_bit_array[i]=1

		# Print the Bloom Filter
		# print(filter_bit_array)
		# print(str(false_positive)+","+str(true_negative)+","+str(true_positive)+","+str(false_negative))
		# print(str(time)+" , "+city+" , "+str(int_city))
		actual_seen_cities.add(city)

	#Print FPR
	FPR=0
	# if((false_positive+true_negative)==0):
	# 	FPR= 0
	# else:
	FPR= float(false_positive/(false_positive+true_negative))
	# print(str(time)+","+str(FPR))
	f= open(out_file, "a")
	f.write("\n"+str(time)+","+str(FPR))
	f.close()


sc = SparkContext("local[2]", "BloomFilter")
ssc = StreamingContext(sc, 10)
current_stream = ssc.socketTextStream("localhost", port_num)

current_stream.foreachRDD(bloom_filtering)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
f.close()