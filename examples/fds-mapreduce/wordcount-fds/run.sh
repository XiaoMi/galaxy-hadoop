#!/usr/bin/env bash

key="" # your AppKey
secret="" # your AppSecret, escape '/' with "%2F"
region="" # fds region
bucket="" # your bucket created in fds, bucket name should not contain '.'
input="input2"  # change to your input, it can be a directory or a file
output="test" # change to your output, result will generated in this dir which will be created at first if not exists.

HADOOP_HOME="" # set your hadoop home

if [ ! -z "$HADOOP_HOME" ];then
  export CLASSPATH_PREFIX=$CLASSPATH_PREFIX:$HADOOP_HOME/conf:$HADOOP_HOME/etc/hadoop
fi

# ------ run in local mode with input data in fds ------
# sh target/wordcount-fds-1.0-SNAPSHOT/bin/wordcount.sh -Dmapreduce.job.user.classpath.first=true -conf job-local.xml fds://$key:$secret@$bucket/$input fds://$key:$secret@$bucket/$output

# ------ run in local mode with input data in local dir------
# sh target/wordcount-fds-1.0-SNAPSHOT/bin/wordcount.sh -Dmapreduce.job.user.classpath.first=true -conf job-local.xml $input fds://$user:$password@$bucket/$output

# ------ run in distributed mode -------
# Attention: for cluster job, you need change Fs.defaultFS to your cluster Fs.defaultFs in job-yarn.xml before run following command  
# or you can just create a new job-yarn-xxx.xml then set option -conf with -conf job-yarn-xxx.xml
# sh target/wordcount-fds-1.0-SNAPSHOT/bin/wordcount.sh -Dmapreduce.job.user.classpath.first=true -conf job-yarn.xml fds://$key:$secret@$bucket/$input fds://$key:$secret@$bucket/$output

# test examples
# sh target/wordcount-fds-1.0-SNAPSHOT/bin/wordcount.sh -Dmapreduce.job.user.classpath.first=true -conf job-yarn-xiaomitst-lpc.xml fds://$key:$secret@$bucket/$input fds://$key:$secret@$bucket/$output
# sh target/wordcount-fds-1.0-SNAPSHOT/bin/wordcount.sh -Dmapreduce.job.user.classpath.first=true -conf job-local.xml fds://$key:$secret@$bucket/$input fds://$key:$secret@$bucket/$output

# ------- run in distributed mode and specify fds region ------
sh target/wordcount-fds-1.0-SNAPSHOT/bin/wordcount.sh -Dmapreduce.job.user.classpath.first=true -conf job-yarn.xml fds://$key:$secret@$bucket.$region/$input fds://$key:$secret@$bucket.$region/$output



