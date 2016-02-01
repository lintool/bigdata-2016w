#!/usr/bin/python
"""CS 489 Big Data Infrastructure (Winter 2016): Self-check script

This file can be open to students

Usage: 
    run this file on 'bigdata2016w' repository with github-username
    ex) check_assignment0_public_linux.py [github-username]
"""

import sys
import os
from subprocess import call
import re

# add prefix 'a' if github-username starts from a numeric character
def convertusername(u):
  return re.sub(r'^(\d+.*)',r'a\1',u)

def check_assignment(u):
    """Run assignment0 in linux environment"""
    call(["mvn","clean","package"])

    # First, convert the adjacency list into PageRank node records:
    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment4.BuildPersonalizedPageRankRecords",
           "-input", "data/p2p-Gnutella08-adj.txt", 
           "-output", "cs489-2016w-"+u+"-a4-Gnutella-PageRankRecords",
           "-numNodes", "6301", "-sources", "367,249,145" ])

    # Next, partition the graph (hash partitioning) and get ready to iterate:
    call("hadoop fs -mkdir cs489-2016w-"+u+"-a4-Gnutella-PageRank",shell=True)
    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment4.PartitionGraph",
           "-input", "cs489-2016w-"+u+"-a4-Gnutella-PageRankRecords", 
           "-output", "cs489-2016w-"+u+"-a4-Gnutella-PageRank/iter0000",
           "-numPartitions", "5", "-numNodes", "6301" ])

    # After setting everything up, iterate multi-source personalized PageRank:
    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment4.RunPersonalizedPageRankBasic",
           "-base", "cs489-2016w-"+u+"-a4-Gnutella-PageRank",
           "-numNodes", "6301", "-start", "0", "-end", "20", "-sources", "367,249,145" ])

    # Finally, run a program to extract the top ten personalized PageRank values, with respect to each source.
    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment4.ExtractTopPersonalizedPageRankNodes",
           "-input", "cs489-2016w-"+u+"-a4-Gnutella-PageRank/iter0020",
           "-output", "cs489-2016w-"+u+"-a4-Gnutella-PageRank-top10",
           "-top", "10", "-sources", "367,249,145" ])
    

if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    u = convertusername(sys.argv[1])
    check_assignment(u)
  except Exception as e:
    print(e)
