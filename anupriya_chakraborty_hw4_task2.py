import sys
from pyspark import SparkConf, SparkContext, SQLContext
from collections import defaultdict
import time
import itertools
from queue import *

start= time.time()
sc = SparkContext()
sqlContext = SQLContext(sc)

filter_threshold= int(sys.argv[1])
data_file = sys.argv[2]
bet_out_file = sys.argv[3]
comm_out_file= sys.argv[4]

def get_user_pairs(x):
	business_id= x[0]
	user_ids= x[1]
	res= []

	pairs= itertools.combinations(user_ids, 2)
	for i in pairs:
		i= sorted(i)
		res.append(((i[0], i[1]), [business_id]))

	return res

def find_edges(x, list_edges):

	res=[]
	for i in list_edges:
		if(i[0]==x):
			res.append(i[1])
		elif(i[1]==x):
			res.append(i[0])
	res= list(set(res))
	return res


def calculate_betweenness(root_node, adjacent_vertices, n_vertices):

	q= Queue(maxsize=n_vertices)
	visited= []
	levels= {}
	parents= {}
	weights= {}

	q.put(root_node)
	visited.append(root_node)
	levels[root_node]= 0
	weights[root_node]= 1
	
	while(q.empty()!=True):
		node= q.get()
		children= adjacent_vertices[node]

		for i in children:
			if(i not in visited):
				q.put(i)
				parents[i]= [node]
				weights[i]= weights[node]
				# print(str(parents[i]))
				visited.append(i)
				levels[i]= levels[node]+1
			else:
				if(i!=root_node):
					parents[i].append(node)
					if(levels[node]==levels[i]-1):
						weights[i]+= weights[node]

	order_v= []
	count=0
	for i in visited:
		order_v.append((i, count))
		count= count+1
	reverse_order= sorted(order_v,key=(lambda x: x[1]),reverse=True)
	rev_order=[]
	nodes_values= {}
	for i in reverse_order:
		rev_order.append(i[0])
		nodes_values[i[0]]= 1
	
	betweenness_values = {}

	for j in rev_order:
		if(j!=root_node):
			total_weight= 0
			for i in parents[j]:
				if(levels[i]==levels[j]-1):
					total_weight+= weights[i]

			for i in parents[j]:
				if(levels[i]==levels[j]-1):
					source= j
					dest= i

					if source<dest :
						pair= tuple((source,dest))
					else:
						pair= tuple((dest,source))


					if(pair not in betweenness_values.keys()):
						betweenness_values[pair]= float(nodes_values[source]*weights[dest]/total_weight)
					else:
						betweenness_values[pair]+= float(nodes_values[source]*weights[dest]/total_weight)

					nodes_values[dest]+= float(nodes_values[source]*weights[dest]/total_weight)

	betweenness_list= []
	for key, value in betweenness_values.items():
		temp = [key,value]
		betweenness_list.append(temp)

	return betweenness_list

def bfs(root_node, adjacent_vertices, n_vertices):
	visited=[]
	edges= []

	q= Queue(maxsize=n_vertices)

	q.put(root_node)
	visited.append(root_node)

	while(q.empty()!=True):
		node= q.get()
		children= adjacent_vertices[node]

		for i in children:
			if(i not in visited):
				q.put(i)
				visited.append(i)

			pair= sorted((node,i))
			if(pair not in edges):
				edges.append(pair)

	return (visited, edges)

def remove_component(remainder_graph, component):
	component_vertices= component[0]
	component_edges= component[1]

	# taking out the vertices
	for v in component_vertices:
		del remainder_graph[v]

	# taking out the edges to other components
	for i in remainder_graph.keys():
		adj_list= remainder_graph[i]
		for v in component_vertices:
			if(v in adj_list):
				adj_list.remove(i[1])
		remainder_graph[i]= adj_list		

	return remainder_graph

def isEmpty(adjacent_vertices):
	if(len(adjacent_vertices)==0):
		return True
	else:
		for i in adjacent_vertices.keys():
			adj_list= adjacent_vertices[i]
			if(len(adj_list)!=0):
				return False
			else:
				pass
		return True

def get_connected_components(adjacent_vertices):

	connected_components= []
	remainder_graph= adjacent_vertices

	while(isEmpty(remainder_graph)==False):
		vertices= []

		for key, value in remainder_graph.items():
			vertices.append(key)

		vertices= list(set(vertices))

		
		root= vertices[0]
		comp_g= bfs(root, adjacent_vertices, len(vertices))
		connected_components.append(comp_g)
		remainder_graph= remove_component(remainder_graph, comp_g)

	return connected_components
	

def calculate_modularity(adjacent_vertices, connected_components, m):

	modularity= 0
	for c in connected_components:
		c_vertices= c[0]
		c_edges= c[1]

		Aij=0
		for i in c_vertices:
			for j in c_vertices:
				Aij=0
				adj_list= adjacent_vertices[str(i)]
				if(j in adj_list):
					Aij=1

				ki= len(adjacent_vertices[i])
				kj= len(adjacent_vertices[j])

				modularity+= Aij-(ki*kj)/(2*m)	

	modularity= modularity/(2*m)
	return modularity

def build_adjacency_matrix(connected_components):

	res= {}
	for c in connected_components:
		c_vertices= c[0]
		c_edges= c[1]

		for i in c_edges:
			if(i[0] in res.keys()):
				res[i[0]].append(i[1])
			else:
				res[i[0]]= [i[1]]

			if(i[1] in res.keys()):
				res[i[1]].append(i[0])
			else:
				res[i[1]]= [i[0]]

	return res	


def remove_edge(adjacency_matrix, first_edge_to_remove):
	if(first_edge_to_remove[0] in adjacency_matrix.keys()):
		l= adjacency_matrix[first_edge_to_remove[0]]
		if(first_edge_to_remove[1] in l):
			l.remove(first_edge_to_remove[1])

	if(first_edge_to_remove[1] in adjacency_matrix.keys()):
		l= adjacency_matrix[first_edge_to_remove[1]]
		if(first_edge_to_remove[0] in l):
			l.remove(first_edge_to_remove[0])
	return adjacency_matrix



read_data= sc.textFile(data_file)
rdd_data= read_data.map(lambda x : x.split(','))
rdd= rdd_data.filter(lambda x: x[0]!= "user_id").persist()

businesswise_users= rdd.map(lambda x: (x[1], [x[0]])).reduceByKey(lambda x,y: x+y)
pairwise_users= businesswise_users.flatMap(lambda x: get_user_pairs(x)).reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1])>=filter_threshold).map(lambda x: x[0])

rdd_vertices= pairwise_users.flatMap(lambda x: [(x[0]),(x[1])]).distinct()
list_vertices= rdd_vertices.collect()
n_vertices= len(list_vertices)

rdd_edges = pairwise_users.map(lambda x: (x[0], x[1])).map(lambda x: (x[0], x[1]))

list_edges= rdd_edges.collect()

adjacent_vertices= rdd_vertices.map(lambda x: (x, find_edges(x, list_edges))).collectAsMap()


betweenness_rdd= rdd_vertices.flatMap(lambda x: calculate_betweenness(x, adjacent_vertices, n_vertices))\
.reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))


first_edge_to_remove= betweenness_rdd.take(1)[0][0]
m= rdd_edges.count()

adjacency_matrix= adjacent_vertices.copy()

connected_components= get_connected_components(adjacency_matrix)
print("LENGTH: "+ str(len(connected_components)))
modularity= calculate_modularity(adjacent_vertices, connected_components, m)
print("MODULARITY: "+str(modularity))

adjacency_matrix= adjacent_vertices.copy()

highest_modularity= -1
communities=[]
count=0
while(1):	
	adjacency_matrix= remove_edge(adjacency_matrix, first_edge_to_remove)
	print("AFTER REMOVAL OF: "+str(first_edge_to_remove))
	connected_components= get_connected_components(adjacency_matrix)
	print("LENGTH: "+ str(len(connected_components)))
	modularity= calculate_modularity(adjacent_vertices, connected_components, m)
	print("MODULARITY: "+str(modularity))

	adjacency_matrix= build_adjacency_matrix(connected_components)

	temp=[]
	for i in adjacency_matrix.keys():
		temp.append(i)
	temp= list(set(temp))
	v_rdd= sc.parallelize(temp)
	betweenness_temp= v_rdd.flatMap(lambda x: calculate_betweenness(x, adjacency_matrix, n_vertices))\
	.reduceByKey(lambda x,y: (x+y)).map(lambda x: (x[0], float(x[1]/2))).sortByKey().map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))
	first_edge_to_remove= betweenness_temp.take(1)[0][0]

	if(modularity>=highest_modularity):
		highest_modularity= modularity
		communities= connected_components

	count+=1
	if(count==50):
		break

sorted_communities= []
for i in communities :
	item= sorted(i[0])
	sorted_communities.append((item,len(item)))

sorted_communities.sort()
sorted_communities.sort(key=lambda x:x[1])



# #====================================================== WRITING TO OUTPUT FILE ======================================================
f = open(bet_out_file, 'w')
ctr=0
for i in betweenness_rdd.collect():
	if(ctr==0):
		ctr=1
	else :
		f.write("\n")
	f.write(str(i[0])+", "+str(i[1]))
f.close()

f = open(comm_out_file, 'w')
ctr=0
for i in sorted_communities:
	if(ctr==0):
		ctr=1
	else :
		f.write("\n")
	s= str(i[0]).replace("[","").replace("]","")
	f.write(s)
f.close()

end= time.time()
print("Duration: "+str(end-start))