# Prerequisite
  Download Hadoop package and unzip
  Set env $HADOOP_HOME to hadoop package root
  <pre><code>export HADOOP_HOME=package_path</code></pre>

# Build
  <pre><code>mvn package</code></pre>

# Run locally
  <pre><code>$HADOOP_HOME/bin/yarn jar target/sds-single-table-many-scans-1.0-SNAPSHOT.jar com.xiaomi.infra.codelab.SDSSingleTableManyScans -Dmapreduce.framework.name=local -Dfs.defaultFS=file:///
        [-Dsds.mapreduce.rest.endpoint=endpoint] [-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] <input table> <output table></code></pre>

# Run on cluster
  <pre><code>$HADOOP_HOME/bin/yarn jar target/sds-single-table-many-scans-1.0-SNAPSHOT.jar com.xiaomi.infra.codelab.SDSSingleTableManyScans [-Dsds.mapreduce.rest.endpoint=endpoint]
        [-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] <input table> <output table></code></pre>
