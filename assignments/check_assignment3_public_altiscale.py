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
import argparse

# add prefix 'a' if github-username starts from a numeric character
def convertusername(u):
  return re.sub(r'^(\d+.*)',r'a\1',u)

def check_a3(username,reducers):
    """Run assignment3 in Altiscale environment"""
    call(["mvn","clean","package"])
    print username
    call(["hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
          "ca.uwaterloo.cs.bigdata2016w.{0}.assignment3.BuildInvertedIndexCompressed".format(username),
          "-input", "/shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt", 
          "-output", "cs489-2016w-{0}-a3-index-wiki".format(username), "-reducers", str(reducers) ])
    print("Question 2.")
    call(["hadoop","fs","-du","-h","cs489-2016w-{0}-a3-index-wiki".format(username)])
    print("Question 3.")
    call(["hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
          "ca.uwaterloo.cs.bigdata2016w.{0}.assignment3.BooleanRetrievalCompressed".format(username),
          "-index", "cs489-2016w-{0}-a3-index-wiki".format(username), 
          "-collection", "/shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt",
          "-query", "waterloo stanford OR cheriton AND"])
    print("Question 4.")
    call(["hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
          "ca.uwaterloo.cs.bigdata2016w.{0}.assignment3.BooleanRetrievalCompressed".format(username),
          "-index", "cs489-2016w-{0}-a3-index-wiki".format(username),
          "-collection", "/shared/cs489/data/enwiki-20151201-pages-articles-0.1sample.txt",
          "-query", "internet startup AND canada AND ontario AND"])
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="CS 489/689 W15 A3 Public Test Script for Altiscale")
  parser.add_argument('username',metavar='[Github Username]',
                      help="Github username to be used as package name",type=str)
  parser.add_argument('-r','--reducers',help="Number of reducers to use.",type=int,default=1)
  args=parser.parse_args()
  try:
    converted_userid = convertusername(args.username)
    check_a3(converted_userid,args.reducers)
  except Exception as e:
    print(e)
