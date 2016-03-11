#!/usr/bin/python
"""CS 489 Big Data Infrastructure (Winter 2016): Assignment 6 Check Script
"""
import sys
import os
from subprocess import call
import argparse
import re

def convertusername(u):
  return re.sub(r'^(\d+.*)',r'a\1',u)

def check_a6(pn,memory, env,iterations):
  spark_cmd = "spark-submit"
  cp_cmd = ["cp"]
  mkdir_cmd = ["mkdir"]
  eval_cmd = "spam_eval.sh"
  data_prefix = "./"
  if env != 'linux':
    spark_cmd = "my-spark-submit"
    cp_cmd = ["hadoop","fs", "-cp"]
    mkdir_cmd = ["hadoop","fs","-mkdir"]
    eval_cmd = "spam_eval_hdfs.sh"
    data_prefix = "/shared/cs489/data"
  else:
    call("cat spam.train.group_x.txt spam.train.group_y.txt spam.train.britney.txt > spam.train.all.txt",shell=True)

  print("0. maven")
  call(["mvn","clean","package"])
  print("1. Train and Classify with Group-X")
  call([spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.TrainSpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.train.group_x.txt".format(data_prefix), 
         "--model", "cs489-2016w-{0}-a6-model-group_x".format(pn)])
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplySpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix), 
         "--model", "cs489-2016w-{0}-a6-model-group_x".format(pn),
         "--output", "cs489-2016w-{0}-a6-output-group_x".format(pn)])
  print("2. Train and Classify  with Group-Y")
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.TrainSpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.train.group_y.txt".format(data_prefix), 
         "--model", "cs489-2016w-{0}-a6-model-group_y".format(pn)])
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplySpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix),
         "--model", "cs489-2016w-{0}-a6-model-group_y".format(pn),
         "--output", "cs489-2016w-{0}-a6-output-group_y".format(pn)])
  print("3. Train and Classify with Britney")
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.TrainSpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.train.britney.txt".format(data_prefix), 
         "--model", "cs489-2016w-{0}-a6-model-britney".format(pn)])
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplySpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix),
         "--model", "cs489-2016w-{0}-a6-model-britney".format(pn),
         "--output", "cs489-2016w-{0}-a6-output-britney".format(pn)])
  print("4. Evaluate Group-X, Group-Y, and Britney")
  call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-group_x".format(pn)])
  call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-group_y".format(pn)])
  call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-britney".format(pn)])
  raw_input('Press <ENTER> to continue.')
  print("5. Voting Ensemble Classify and Evaluate")
  call(mkdir_cmd +["cs489-2016w-{0}-a6-model-fusion".format(pn)])
  call(cp_cmd + ["cs489-2016w-{0}-a6-model-group_x/part-00000".format(pn), "cs489-2016w-{0}-a6-model-fusion/part-00000".format(pn)])
  call(cp_cmd + ["cs489-2016w-{0}-a6-model-group_y/part-00000".format(pn), "cs489-2016w-{0}-a6-model-fusion/part-00001".format(pn)])
  call(cp_cmd + ["cs489-2016w-{0}-a6-model-britney/part-00000".format(pn), "cs489-2016w-{0}-a6-model-fusion/part-00002".format(pn)])
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplyEnsembleSpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix),
         "--model", "cs489-2016w-{0}-a6-model-fusion".format(pn),
         "--output", "cs489-2016w-{0}-a6-output-fusion-average".format(pn),
         "--method","average"])
  call([spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplyEnsembleSpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix),
         "--model", "cs489-2016w-{0}-a6-model-fusion".format(pn),
         "--output", "cs489-2016w-{0}-a6-output-fusion-voting".format(pn),
         "--method","vote"])
  call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-fusion-average".format(pn)])
  call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-fusion-voting".format(pn)])
  raw_input('Press <ENTER> to continue.')
  print("6. Batch Training")
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.TrainSpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.train.all.txt".format(data_prefix), 
         "--model", "cs489-2016w-{0}-a6-model-all".format(pn)])
  call([ spark_cmd,"--driver-memory",memory,
         "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplySpamClassifier".format(pn),
         "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix),
         "--model", "cs489-2016w-{0}-a6-model-all".format(pn),
         "--output", "cs489-2016w-{0}-a6-output-all".format(pn)])
  call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-all".format(pn)])
  raw_input('Press <ENTER> to continue.')
  print("6. Data Shuffling ({0} iterations)".format(iterations))
  for i in range(iterations):
    call([ spark_cmd,"--driver-memory",memory,
           "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.TrainSpamClassifier".format(pn),
           "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.train.britney.txt".format(data_prefix), 
           "--model", "cs489-2016w-{0}-a6-model-britney-shuffle".format(pn),
           "--shuffle"])
    call([ spark_cmd,"--driver-memory",memory,
           "--class", "ca.uwaterloo.cs.bigdata2016w.{0}.assignment6.ApplySpamClassifier".format(pn),
           "target/bigdata2016w-0.1.0-SNAPSHOT.jar", "--input", "{0}/spam.test.qrels.txt".format(data_prefix),
           "--model", "cs489-2016w-{0}-a6-model-britney-shuffle".format(pn),
           "--output", "cs489-2016w-{0}-a6-output-britney-shuffle".format(pn)])
    call(["bash",eval_cmd,"cs489-2016w-{0}-a6-output-britney-shuffle".format(pn)])
    raw_input('Press <ENTER> to continue.')

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="CS 489/689 W15 A6 Public Test Script")                                                         
  parser.add_argument('username',metavar='Github Username',
                      help="Github username to be used as package name",type=str)
  parser.add_argument('-m','--memory',help="Amount of memory to give Spark jobs",type=str,default="2G")
  parser.add_argument('-e', '--env', help='Environment to test under.',type=str,default='linux')
  parser.add_argument('-i', '--iterations', help='Number of shuffle iterations',type=int,default=1)
  args=parser.parse_args()
  try:
    converted_userid = convertusername(args.username)
    check_a6(converted_userid,args.memory,args.env,args.iterations)
  except Exception as e:
    print(e)
