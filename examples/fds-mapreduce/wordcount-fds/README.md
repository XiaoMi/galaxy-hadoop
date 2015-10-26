FDS Examples
---
###Prerequisite
fds-mapreduce job has dependencies on galaxy-hadoop and other XiaoMi github project like galaxy-sdk-java and galaxy-fds-sdk-java, you should mvn install them first.
```
git clone https://github.com/XiaoMi/galaxy-sdk-java.git  
cd galaxy-sdk-java  
mvn clean install -DskipTests

git clone https://github.com/XiaoMi/galaxy-fds-sdk-java.git
cd galaxy-fds-sdk-java
mvn clean install -DskipTests

git clone https://github.com/XiaoMi/galaxy-hadoop.git
cd galaxy-hadoop/
mvn clean install -DskipTests
```

###Build
For cluster job
```
cd galaxy-hadoop/examples/fds-mapreduce/wordcount-fds/
cp /path/to/yourclusterconffiles ./conf # cluster conf files are core-site.xml, hdfs-site.xml, mapred-site.xml, yarn-site.xml
mvn clean package -DskipTests -Phadoop2.4
```
For local job
```
cd galaxy-hadoop/examples/fds-mapreduce/wordcount-fds/
mvn clean package -DskipTests -Phadoop2.4
```
Currently, EMR clusters are depolyed with hadoop2.4, but we may support hadoop2.0 in the near future.
So in the future you can use -Phadoop2.0 for emr cluster with hadoop2.0

###JobSubmission
```
sh run.sh # you must follow comments in run.sh to finish configuration in run.sh before execute this command

```


