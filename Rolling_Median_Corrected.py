
import json
import datetime
import time
from collections import defaultdict 
import numpy 

# Initialize a default dictionary that will be used.
venmoDict = defaultdict(list)
f = open('RollingMedian.txt', 'w') 

# Creates the 60 sec data timeframe for incoming venmo messages
def timed_graph(graph,unix_time):
    for target,actors in graph.items():
        if len(actors) == 0:
            del graph[target]
        i = 0
        while i < len(actors): 
            actname, transtime = actors[i] 
            if unix_time - transtime >= 60: # Difference between created time and running time.
                graph[target].pop(i) 
                # need to do: if target values empty remove target
            else:
                i +=1
    return graph 
    
# This function delete multiple edges/transactions between the same actor and target.
def make_unique(graph):
    for target in graph.keys():
        seen = set()
        graph[target] = [x for x in graph[target] if tuple(x[0]) not in seen and not seen.add(tuple(x[0]))]
    return graph   
    
# For when the target and actor values are interchanged, we need to remove repeated edges.    
def remove_cross(graph):
    for target,actors in graph.items():
        for i in range(len(actors)): # Looping through actors tuple
	   actorname = actors[i][0] 
	   j = 0
	   if actorname in graph.keys(): # For every actor that is a target, check their corresponding actors
		actors1 = graph[actorname]
		while j < len(actors1): # looping through corresponding actors
		    actorname1 = actors1[j][0]
		    if actorname1 == target:  # If corresponding actor is also similar to taget than remove
		        actors1.pop(j) # removes the first entry in the jth position
		    else:
			j+= 1
    return graph
    
with open('venmo-trans.txt', 'r') as f_in:   
    for line in f_in:
        contents = json.loads(line) 
        time_stamp = datetime.datetime.strptime(contents["created_time"], "%Y-%m-%dT%H:%M:%SZ") # Strip date and time from json content
        unix_time = time.mktime(time_stamp.timetuple()) # Standardize time to seconds starting from 01/01/1970
        # Creates a dictionary with target as key and a tuple of actor and it's created time as values
        venmoDict[contents["target"]].append((contents["actor"],unix_time))      
        # cleaning and timing data frame          
        framedgraph = timed_graph(venmoDict,unix_time)        
        uniquegraph = make_unique(framedgraph)
        crossgraph = remove_cross(uniquegraph)
                
        neighbor = [] # To store the degree of each node/target
        for actors in crossgraph.values():
            neighbor.append(len(actors))
    
        result = numpy.median(neighbor) # Calculates median after sorting neighbors
        result = "%.2f" % result # Rounds to two decimal place
        f.write(str(result) + "\n" ) 

f_in.close()
