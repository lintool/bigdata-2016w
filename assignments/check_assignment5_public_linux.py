#!/usr/bin/python
"""CS 489 Big Data Infrastructure (Winter 2016): Self-check script

This file can be open to students

Usage: 
    run this file on 'bigdata2016w' repository with github-username
    ex) check_assignment5_public_linux.py [github-username]
"""

import sys
import os
from subprocess import call
import re

# add prefix 'a' if github-username starts from a numeric character
def convertusername(u):
  return re.sub(r'^(\d+.*)',r'a\1',u)

def check_a5(u):
    """Run assignment5 in linux environment"""
    call(["mvn","clean","package"])

    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q1",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT", "--date", "1996-01-01"])

    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q2",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT", "--date", "1996-01-01"])

    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q3",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT", "--date", "1996-01-01"])
    
    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q4",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT", "--date", "1996-01-01"])
    
    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q5",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT"])

    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q6",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT", "--date", "1996-01-01"])

    call([ "spark-submit", "--class", "ca.uwaterloo.cs.bigdata2016w."+u+".assignment5.Q7",
 	   "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "TPC-H-0.1-TXT", "--date", "1996-01-01"])
    
if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    u = convertusername(sys.argv[1])
    check_a5(u)
  except Exception as e:
    print(e)
