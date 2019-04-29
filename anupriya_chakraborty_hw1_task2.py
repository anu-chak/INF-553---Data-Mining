import sys
from pyspark import SparkConf, SparkContext
import json
import time

# ================================================= Start of Execution =================================================
ts_start= time.time();
sc = SparkContext()

data_file = sys.argv[1]
out_file = sys.argv[2]
num_partitions = int(sys.argv[3])

read_data = sc.textFile(data_file)
rdd=read_data.map(json.loads).map(lambda row:(row['business_id'],row['business_id'])).persist()

def count_items_per_partition(index, i) :
	ctr= 0
	for _ in i:
		ctr= ctr+1
	return index, ctr

# ================================================= End of Data Load ==================================================
ts_part1_start= time.time();
myList1= rdd.mapPartitionsWithIndex(count_items_per_partition).collect()
list1= myList1[1::2]

ts_start1= time.time()
business_list1= rdd.map(lambda row:(row[0],1)).reduceByKey(lambda a,b: a+b)
business_list1_sorted= business_list1.sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))
ts_end1= time.time()

# ============================================== End of Part 1 Execution ==============================================
ts_part2_start= time.time();
def myPartitioner(business_id):
	return hash(business_id)

rdd2 = rdd.partitionBy(num_partitions, myPartitioner)

myList2= rdd2.mapPartitionsWithIndex(count_items_per_partition).collect()
list2= myList2[1::2]

ts_start2= time.time()
business_list2= rdd.map(lambda row:(row[0],1)).reduceByKey(lambda a,b: a+b)
business_list2_sorted= business_list2.sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))
ts_end2= time.time()

# ============================================== End of Part 2 Execution ==============================================
ts_write_start= time.time();
f = open(out_file, 'w')
f.write("{")
f.write("\"default\":{")
f.write("\"n_partition\":"+str(rdd.getNumPartitions())+",")
f.write("\"n_items\":"+str(list1)+",")
f.write("\"exe_time\":"+str(ts_end1-ts_start1))
f.write("},")
f.write("\"customized\":{")
f.write("\"n_partition\":"+str(rdd2.getNumPartitions())+",")
f.write("\"n_items\":"+str(list2)+",")
f.write("\"exe_time\":"+str(ts_end2-ts_start2))
f.write("},")
f.write("\"explanation\":\"The custom partitioner uses a hash partitioner to ensure that all the records with the same keys are handled by the same reduce node by calculating a unique hash value for a key. By ensuring that the records for the same key are handled by the same reducer, the custom partitioner eliminates the need for the shuffle phase in which all the records under the same key are grouped together and consequently, the execution time required for this phase. This results in significant reduction of execution time of map reduce operations.\"")
f.write("}")

f.close()
