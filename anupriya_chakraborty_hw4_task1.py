import sys
from pyspark import SparkConf, SparkContext, SQLContext
from collections import defaultdict
import time
import itertools
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from graphframes import *

start= time.time()
sc = SparkContext()
sqlContext = SQLContext(sc)

filter_threshold= int(sys.argv[1])
data_file = sys.argv[2]
out_file = sys.argv[3]

def get_user_pairs(x):
	business_id= x[0]
	user_ids= x[1]
	res= []

	pairs= itertools.combinations(user_ids, 2)
	for i in pairs:
		i= sorted(i)
		res.append(((i[0], i[1]), [business_id]))

	return res

# def getUserByBusiness(e):
#     for m in itertools.combinations(e[1], 2):
#         m = sorted(m)
#         yield ([(m[0], m[1]), [e[0]]])
# ======================================================= DRIVER PROGRAM START  ======================================================
# pairwise_users = sc.textFile(data_file).map(lambda e: e.split(","))\
#     .filter(lambda e: e[0] != "user_id").map(lambda e: (e[1], [e[0]]))\
#     .reduceByKey(lambda x,y: x+y).flatMap(getUserByBusiness).reduceByKey(lambda x,y: x+y)\
#     .filter(lambda e: len(set(e[1])) >= filter_threshold).map(lambda e: e[0]).persist().cache()


read_data= sc.textFile(data_file)
rdd_data= read_data.map(lambda x : x.split(','))
rdd= rdd_data.filter(lambda x: x[0]!= "user_id").persist()

businesswise_users= rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)
pairwise_users= businesswise_users.flatMap(lambda x: get_user_pairs(x)).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>=filter_threshold).map(lambda x: x[0])

# for i in pairwise_users.take(10):
# 	print(str(i))

# print("Length: "+str(pairwise_users.count()))


v_schema = StructType([StructField('id', StringType())])
rdd_vertices= pairwise_users.flatMap(lambda x: [Row(x[0]),Row(x[1])]).distinct()
df_vertices= sqlContext.createDataFrame(rdd_vertices, v_schema)

# for i in df_vertices.take(10):
# 	print(str(i))
# print("Length V: "+str(df_vertices.count()))


e_schema = StructType([StructField('src', StringType()), StructField('dst',StringType())])
rdd_edges = pairwise_users.map(lambda x: (x[0], x[1])).map(lambda x: Row(src=x[0], dst=x[1]))
df_edges= sqlContext.createDataFrame(rdd_edges, e_schema)

# for i in df_edges.take(10):
# 	print(str(i))
# print("Length E: "+str(df_edges.count()))

g = GraphFrame(df_vertices,df_edges)
graph_df= g.labelPropagation(maxIter=5).rdd.map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: x[1])
graph_list= graph_df.collect()
sorted_graph_list=[]
for i in graph_list:
	j= sorted(i)
	sorted_graph_list.append((j,len(j)))
sorted_graph_list.sort(key=lambda x: x[0])
sorted_graph_list.sort(key=lambda x: x[1])



# #====================================================== WRITING TO OUTPUT FILE ======================================================
f = open(out_file, 'w')

for i in sorted_graph_list:
	s= str(i[0]).replace("[","").replace("]","")
	f.write(str(s))
	f.write("\n")
# f.write("VERTICES")
# for i in df_vertices.collect():
# 	f.write("\n")
# 	f.write(str(i))

# f.write("\n\n\n")

# f.write("EDGES")
# for i in df_edges.collect():
# 	f.write("\n")
# 	f.write(str(i))
f.close()

end= time.time()
print("Duration: "+str(end-start))