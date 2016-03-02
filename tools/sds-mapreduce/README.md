# Prerequisite
  Download Hadoop package and unzip
  Set env $HADOOP_HOME to hadoop package root
  <pre><code>export HADOOP_HOME=package_path</code></pre>

# Build
  <pre><code>mvn package</code></pre>

# Run locally
  <pre><code>$HADOOP_HOME/bin/yarn jar target/sds-rebuild-index-tool-1.0-SNAPSHOT.jar com.xiaomi.infra.galaxy.SDSRebuildIndexTool -Dmapreduce.framework.name=local -Dfs.defaultFS=file:///
        [-Dsds.mapreduce.rest.endpoint=endpoint] [-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] <rebuilt table></code></pre>

# Run on cluster
  <pre><code>$HADOOP_HOME/bin/yarn jar target/sds-rebuild-index-tool-1.0-SNAPSHOT.jar com.xiaomi.infra.galaxy.SDSRebuildIndexTool [-Dsds.mapreduce.rest.endpoint=endpoint]
        [-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] <rebuilt table></code></pre>
