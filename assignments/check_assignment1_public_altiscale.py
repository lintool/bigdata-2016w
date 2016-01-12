#!/usr/bin/python
"""CS 489 Big Data Infrastructure (Winter 2016): Self-check script

This file can be open to students

Usage: 
    run this file on 'bigdata2016w' repository with github-username
    ex) check_assignment1_public_altiscale.py [github-username]
"""

import sys
import os
from subprocess import call
import re

# add prefix 'a' if github-username starts from a numeric character
def convertusername(u):
  return re.sub(r'^(\d+.*)',r'a\1',u)

def check_a1(u):
    """Run assignment1 in linux environment"""
    call(["mvn","clean","package"])

    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment1.PairsPMI",
           "-input", "/shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt", 
           "-output", "cs489-2016w-"+u+"-a1-wiki-pairs", "-reducers", "5" ])
    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment1.StripesPMI",
           "-input", "/shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt", 
           "-output", "cs489-2016w-"+u+"-a1-wiki-stripes", "-reducers", "5" ])
    print("Question 7.")
    call("hadoop fs -cat cs489-2016w-"+u+"-a1-wiki-pairs/part-r-0000* | grep '(waterloo,' |  grep -v 'E-' | sort -k 3 -n -r | head -3",shell=True)
    call("hadoop fs -cat cs489-2016w-"+u+"-a1-wiki-pairs/part-r-0000* | grep '(toronto,' |  grep -v 'E-' | sort -k 3 -n -r | head -3",shell=True)
	
if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    u = convertusername(sys.argv[1])
    check_a1(u)
  except Exception as e:
    print(e)
