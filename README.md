Galaxy Hadoop使用介绍
========================
##### 编译安装到本地(或发布到Maven仓库)

编译依赖galaxy-sdk-java
```
git clone https://github.com/XiaoMi/galaxy-sdk-java.git
cd galaxy-sdk-java
mvn clean install -DskipTests
```

编译依赖galaxy-fds-sdk-java
```
git clone https://github.com/XiaoMi/galaxy-fds-sdk-java.git
cd galaxy-fds-sdk-java
mvn clean install -DskipTests
```
编译galaxy-hadoop
```
mvn clean install
```

##### 如果项目使用Maven进行依赖管理，在项目的pom.xml文件中加入`galaxy-hadoop`依赖：

```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-hadoop</artifactId>
      <version>1.6-SNAPSHOT</version>
    </dependency>
```


Galaxy Hadoop User Guide
========================
##### Build the source code and install the jars into local maven repository (or deploy to a central Maven repository)

compile dependent library galaxy-sdk-java
```
git clone https://github.com/XiaoMi/galaxy-sdk-java.git
cd galaxy-sdk-java
mvn clean install -DskipTests
```

compile dependent library galaxy-fds-sdk-java
```
git clone https://github.com/XiaoMi/galaxy-fds-sdk-java.git
cd galaxy-fds-sdk-java
mvn clean install -DskipTests
```

compile galaxy-hadoop
```
mvn clean install
```

##### Import the above jars into the project classpath or add the following dependency if your project is managed with Maven:

```
    <dependency>
      <groupId>com.xiaomi.infra.galaxy</groupId>
      <artifactId>galaxy-hadoop</artifactId>
      <version>1.6-SNAPSHOT</version>
    </dependency>
```

