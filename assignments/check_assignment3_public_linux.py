#!/usr/bin/python
"""CS 489 Big Data Infrastructure (Winter 2016): Self-check script

This file can be open to students

Usage: 
    run this file on 'bigdata2016w' repository with github-username
    ex) check_assignment3_public_linux.py [github-username]
"""

import sys,os,re,argparse
from subprocess import call

# add prefix 'a' if github-username starts from a numeric character
def convertusername(u):
  return re.sub(r'^(\d+.*)',r'a\1',u)

def check_a3(username,reducers):
    """Run assignment3 in linux environment"""
    call(["mvn","clean","package"])
    call(["hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
          "ca.uwaterloo.cs.bigdata2016w.{0}.assignment3.BuildInvertedIndexCompressed".format(username),
          "-input", "data/Shakespeare.txt", 
          "-output", "cs489-2016w-{0}-a3-index-shakespeare".format(username), "-reducers", str(reducers) ])
    print("Question 1.")
    call(["du","-h","cs489-2016w-{0}-a3-index-shakespeare".format(username)])
    call(["hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
          "ca.uwaterloo.cs.bigdata2016w.{0}.assignment3.BooleanRetrievalCompressed".format(username),
          "-index","cs489-2016w-{0}-a3-index-shakespeare".format(username),
          "-collection","data/Shakespeare.txt","-query", "outrageous fortune AND"])
    call(["hadoop","jar","target/bigdata2016w-0.1.0-SNAPSHOT.jar",
          "ca.uwaterloo.cs.bigdata2016w.{0}.assignment3.BooleanRetrievalCompressed".format(username),
          "-index", "cs489-2016w-{0}-a3-index-shakespeare".format(username), 
          "-collection","data/Shakespeare.txt",
          "-query", "white red OR rose AND pluck AND"])

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="CS 489/689 W15 A3 Public Test Script for Linux")                                                         
  parser.add_argument('username',metavar='[Github Username]',
                      help="Github username to be used as package name",type=str)
  parser.add_argument('-r','--reducers',help="Number of reducers to use.",type=int,default=1)
  args=parser.parse_args()
  try:
    converted_userid = convertusername(args.username)
    check_a3(converted_userid,args.reducers)
  except Exception as e:
    print(e)
