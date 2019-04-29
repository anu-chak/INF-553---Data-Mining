import sys
from pyspark import SparkConf, SparkContext
import json
import time

sc = SparkContext()

review_file = sys.argv[1]
business_file = sys.argv[2]
out_file_a = sys.argv[3]
out_file_b = sys.argv[4]

read_reviews = sc.textFile(review_file)
rdd_rev = read_reviews.map(json.loads).map(lambda row:(row['business_id'], row['stars'])).persist()

read_business = sc.textFile(business_file)
rdd_bus = read_business.map(json.loads).map(lambda row:(row['business_id'], row['city'])).persist()

rdd_rb = rdd_bus.join(rdd_rev).map(lambda a: a[1])

rdd_agg = rdd_rb.aggregateByKey((0,0), lambda a,b: (a[0]+b, a[1]+1), lambda a,b: (a[0]+b[0], a[1]+b[1]))
rdd_average = rdd_agg.mapValues(lambda av: av[0]/av[1]).sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a:(a[1],a[0]))

# =========================================================== METHOD 1 ===========================================================

ts_start_a= time.time()

res_a= rdd_average.collect();
index= 1

for x in res_a :
	print(str(x))
	if index==10 : 
		break
	index= index+1

# =========================================================== METHOD 2 ===========================================================
ts_start_b= time.time()

res_b= rdd_average.take(10);
for x in res_b : 
	print(str(x))

ts_end_b= time.time()

# =================================================== WRITING TO OUTPUT FILE A ===================================================

f_a = open(out_file_a, 'w')
f_a.write("city,stars\n")
index= 0

for x in res_a : 
	if index!=0 :
		f_a.write("\n")
	else :
		index= 1
	f_a.write(str(x[0])+","+str(x[1]))

f_a.close()

# =================================================== WRITING TO OUTPUT FILE B ===================================================

f_b = open(out_file_b, 'w')
f_b.write("{\"m1\":"+str(ts_start_b-ts_start_a)+",")
f_b.write("\"m2\":"+str(ts_end_b-ts_start_b)+",")
f_b.write("\"explanation\":\"As we can see, it takes much longer to first collect and then print the top 10 records than when we take the top 10 records and print them. This is because collect() does a full pass over the entire RDD and then converts it to a list, from which we print the top 10 records. This is the approach we followed in Method 1. In the case of take(10), the top 10 records from the RDD are directly converted into a list without the need for a full pass over the RDD. We only print the elements of this list in Method 2. This explains why Method 2 takes lesser execution time than Method 1.\"}")
f_b.close()
