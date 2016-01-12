#!/usr/bin/python
"""CS 489 Big Data Infrastructure (Winter 2016): Self-check script

This file can be open to students

Usage: 
    run this file on 'bigdata2016w' repository with github-username
    ex) check_assignment1_public_linux.py [github-username]
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
           "-input", "data/Shakespeare.txt", 
           "-output", "cs489-2016w-"+u+"-a1-shakespeare-pairs", "-reducers", "5" ])
    call([ "hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
           "ca.uwaterloo.cs.bigdata2016w."+u+".assignment1.StripesPMI",
           "-input", "data/Shakespeare.txt", 
           "-output", "cs489-2016w-"+u+"-a1-shakespeare-stripes", "-reducers", "5" ])
    print("Question 4.")
    call("cat cs489-2016w-"+u+"-a1-shakespeare-pairs/part-r-0000* | wc",shell=True)
    print("Question 5.")
    call("cat cs489-2016w-"+u+"-a1-shakespeare-pairs/part-r-0000* | grep -v 'E-' | sort -k 3 -n -r | head",shell=True)
    print("Question 6.")
    call("cat cs489-2016w-"+u+"-a1-shakespeare-pairs/part-r-0000* | grep '(tears,' |  grep -v 'E-' | sort -k 3 -n -r | head -3",shell=True)
    call("cat cs489-2016w-"+u+"-a1-shakespeare-pairs/part-r-0000* | grep '(death,' |  grep -v 'E-' | sort -k 3 -n -r | head -3",shell=True)

if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    u = convertusername(sys.argv[1])
    check_a1(u)
  except Exception as e:
    print(e)
