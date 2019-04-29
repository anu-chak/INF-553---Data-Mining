
import sys
from pyspark import SparkConf, SparkContext
import json
import time

start= time.time()
sc = SparkContext()

data_file = sys.argv[1]
out_file = sys.argv[2]

read_data = sc.textFile(data_file)
rdd=read_data.map(json.loads).map(lambda row:(row['user_id'],row['business_id'],row['date'])).persist()

# =========================================================== ANSWER A ===========================================================

total_count= rdd.count()

# =========================================================== ANSWER B ===========================================================

relevant_fields= rdd.map(lambda row:(row[2])).filter(lambda x: x[:4]=="2018")
rel_ctr= relevant_fields.count()

# =========================================================== ANSWER C ===========================================================

user_list= rdd.map(lambda row:(row[0]))
user_list_distinct= user_list.distinct()

ctr_users= user_list_distinct.count()

# =========================================================== ANSWER D ===========================================================

user_list_red= user_list.map(lambda row:(row,1)).reduceByKey(lambda a,b: a+b)
user_list_sorted= user_list_red.sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))

ctr_user_list_red= user_list_sorted.take(10)
out_str_d= str(ctr_user_list_red).replace('(','[').replace(')',']').replace('\'','\"')

# =========================================================== ANSWER E ===========================================================

business_list= rdd.map(lambda row:(row[1]))
business_list_distinct= business_list.distinct()

ctr_businesses= business_list_distinct.count()

# =========================================================== ANSWER F ===========================================================

business_list_red= business_list.map(lambda row:(row,1)).reduceByKey(lambda a,b: a+b)
business_list_sorted= business_list_red.sortByKey(ascending=True).map(lambda a:(a[1],a[0])).sortByKey(ascending=False).map(lambda a: (a[1],a[0]))

ctr_business_list_red= business_list_sorted.take(10)
out_str_f= str(ctr_business_list_red).replace('(','[').replace(')',']').replace('\'','\"')

# ====================================================== GENERATING OUTPUTS ======================================================
f = open(out_file, 'w')
f.write("{\"n_review\":"+str(total_count)+",")
f.write("\"n_review_2018\":"+str(rel_ctr)+",")
f.write("\"n_user\":"+str(ctr_users)+",")
f.write("\"top10_user\":"+out_str_d+",")
f.write("\"n_business\":"+str(ctr_businesses)+",")
f.write("\"top10_business\":"+out_str_f+"}")
f.close()
