SEDGE
=====

*SEDGE* is a dynamic symbolic execution system, that generates test cases to exercise the key behavior of the operators that comprise a data flow program. Please read [our paper](http://people.cs.umass.edu/~kaituo/) for more information. 

The latest release is compatible with Apache Pig 0.9.2.

License
-------

*SEDGE* is open source under the [MIT license](http://www.opensource.org/licenses/mit-license.php).

*SEDGE* is built on top of the following third-party libraries. We are thankful to all individuals that have created these. Cross references to these licensing terms are given with the applicable items in the list. 
* [z3](http://z3.codeplex.com/license)
* [Apache Pig](http://www.apache.org/licenses/)
* [Xeger](http://www.apache.org/licenses/LICENSE-2.0)
* [Automaton](http://opensource.org/licenses/bsd-license.php)
* [Coral](http://javapathfinder.sourceforge.net/NOSA-1.3-JPF.txt)
* [Apache Commons](http://www.apache.org/licenses/)

Download
--------
Download pre-compiled binaries and all required third-party libraries for Linux here:
[V0.1](https://docs.google.com/file/d/0B2eUVi06EB0oenJ6UzdtNFJoX0U/edit?usp=sharing)

Installation
------------
1. Download the binary. 
2. Change directory to the location where you would like *SEDGE* to be installed. We'll call this location the SEDGEDIR directory. Move the .tar.gz archive binary to the SEDGEDIR directory.

3. Unpack the tarball.

    % tar zxvf sedge-binary-\<version\>.tar.gz

*SEDGE* is installed in a directory called sedge-binary-\<version\>.tar.gz in the SEDGEDIR directory.
4. You must install the JDK in your operating system. The Java 7 is recommended. 
5. Update the environment variable CLASSPATH and LD_LIBRARY_PATH.
```bash
export CLASSPATH=$SEDGEDIR/lib/ant-1.6.5.jar:$SEDGEDIR/lib/antlr-2.7.7.jar:$SEDGEDIR/lib/antlr-3.4.jar:$SEDGEDIR/lib/antlr-runtime-3.4.jar:$SEDGEDIR/lib/automaton-1.11-8.jar:$SEDGEDIR/lib/avro-1.5.3.jar:$SEDGEDIR/lib/commons-beanutils-1.7.0.jar:$SEDGEDIR/lib/commons-beanutils-core-1.8.0.jar:$SEDGEDIR/lib/commons-cli-1.2.jar:$SEDGEDIR/lib/commons-codec-1.4.jar:$SEDGEDIR/lib/commons-collections-3.2.1.jar:$SEDGEDIR/lib/commons-configuration-1.6.jar:$SEDGEDIR/lib/commons-digester-1.8.jar:$SEDGEDIR/lib/commons-el-1.0.jar:$SEDGEDIR/lib/commons-httpclient-3.0.1.jar:$SEDGEDIR/lib/commons-io-2.4.jar:$SEDGEDIR/lib/commons-lang-2.4.jar:$SEDGEDIR/lib/commons-logging-1.1.1.jar:$SEDGEDIR/lib/commons-math-1.2.jar:$SEDGEDIR/lib/commons-math-2.1.jar:$SEDGEDIR/lib/commons-math3-3.0.jar:$SEDGEDIR/lib/commons-net-1.4.1.jar:$SEDGEDIR/lib/coral.jar:$SEDGEDIR/lib/coral-related-jpf.jar:$SEDGEDIR/lib/core-3.1.1.jar:$SEDGEDIR/lib/ftplet-api-1.0.0.jar:$SEDGEDIR/lib/ftpserver-core-1.0.0.jar:$SEDGEDIR/lib/ftpserver-deprecated-1.0.0-M2.jar:$SEDGEDIR/lib/guava-11.0.jar:$SEDGEDIR/lib/hadoop-0.23.0-gridmix.jar:$SEDGEDIR/lib/hadoop-0.23.0-streaming.jar:$SEDGEDIR/lib/hadoop-core-1.0.0.jar:$SEDGEDIR/lib/hadoop-mapreduce-0.23.0.jar:$SEDGEDIR/lib/hadoop-mapreduce-0.23.0-sources.jar:$SEDGEDIR/lib/hadoop-mapreduce-examples-0.23.0.jar:$SEDGEDIR/lib/hadoop-mapreduce-examples-0.23.0-sources.jar:$SEDGEDIR/lib/hadoop-mapreduce-test-0.23.0.jar:$SEDGEDIR/lib/hadoop-mapreduce-test-0.23.0-sources.jar:$SEDGEDIR/lib/hadoop-mapreduce-tools-0.23.0.jar:$SEDGEDIR/lib/hadoop-mapreduce-tools-0.23.0-sources.jar:$SEDGEDIR/lib/hadoop-test-1.0.0.jar:$SEDGEDIR/lib/hamcrest-all-1.3.0RC2.jar:$SEDGEDIR/lib/hbase-0.90.0.jar:$SEDGEDIR/lib/hbase-0.90.0-tests.jar:$SEDGEDIR/lib/hive-exec-0.8.0.jar:$SEDGEDIR/lib/hsqldb-1.8.0.10.jar:$SEDGEDIR/lib/httpclient-4.1.jar:$SEDGEDIR/lib/httpcore-4.1.jar:$SEDGEDIR/lib/jackson-core-asl-1.7.3.jar:$SEDGEDIR/lib/jackson-mapper-asl-1.7.3.jar:$SEDGEDIR/lib/jasper-compiler-5.5.12.jar:$SEDGEDIR/lib/jasper-runtime-5.5.12.jar:$SEDGEDIR/lib/javacc-4.2.jar:$SEDGEDIR/lib/javacc.jar:$SEDGEDIR/lib/jdeb-0.8.jar:$SEDGEDIR/lib/jersey-core-1.8.jar:$SEDGEDIR/lib/jets3t-0.7.1.jar:$SEDGEDIR/lib/jetty-6.1.26.jar:$SEDGEDIR/lib/jetty-util-6.1.26.jar:$SEDGEDIR/lib/jline-0.9.94.jar:$SEDGEDIR/lib/joda-time-1.6.jar:$SEDGEDIR/lib/js-1.7R2.jar:$SEDGEDIR/lib/jsch-0.1.38.jar:$SEDGEDIR/lib/json-simple-1.1.jar:$SEDGEDIR/lib/jsp-2.1-6.1.14.jar:$SEDGEDIR/lib/jsp-api-2.1-6.1.14.jar:$SEDGEDIR/lib/junit-3.8.1.jar:$SEDGEDIR/lib/junit-4.5.jar:$SEDGEDIR/lib/jython-2.5.0.jar:$SEDGEDIR/lib/kfs-0.3.jar:$SEDGEDIR/lib/log4j-1.2.16.jar:$SEDGEDIR/lib/mina-core-2.0.0-M5.jar:$SEDGEDIR/lib/netty-3.2.2.Final.jar:$SEDGEDIR/lib/opt4j-2.4.jar:$SEDGEDIR/lib/org.hamcrest.core_1.1.0.v20090501071000.jar:$SEDGEDIR/lib/oro-2.0.8.jar:$SEDGEDIR/lib/paranamer-2.3.jar:$SEDGEDIR/lib/sdsuLibJKD12.jar:$SEDGEDIR/lib/servlet-api-2.5-20081211.jar:$SEDGEDIR/lib/servlet-api-2.5-6.1.14.jar:$SEDGEDIR/lib/slf4j-api-1.6.1.jar:$SEDGEDIR/lib/slf4j-log4j12-1.6.1.jar:$SEDGEDIR/lib/snappy-java-1.0.3.2.jar:$SEDGEDIR/lib/ST4-4.0.4.jar:$SEDGEDIR/lib/stringtemplate-3.2.1.jar:$SEDGEDIR/lib/trove-2.1.0.jar:$SEDGEDIR/lib/trove-3.0.3.jar:$SEDGEDIR/lib/xmlenc-0.52.jar:$SEDGEDIR/lib/z3.jar:$SEDGEDIR/lib/zookeeper-3.3.3.jar:$SEDGEDIR/sedge.jar:$SEDGEDIR/lib/coral-related-jpf.jar:.:$CLASSPATH

export LD_LIBRARY_PATH=$SEDGEDIR/z3/
```

Basic Usage
-----------
To generate example data for a Pig script using *SEDGE*, write the script inside a JUnit test method, register each Pig statement using PigServer.registerQuery(), and call PigServer.getExamples2() on a root operator.  In addition, if you have sampled real data, you can pass the path of the data file to each LOAD function using approapriate schema; if you don't have sampled real data, you also need to pass in the path of an empty file to each LOAD function.  See the [testFilter1](https://docs.google.com/file/d/0B2eUVi06EB0oN3RtQ2cxUncyZWs/edit?usp=sharing) example:

```java
    @Test
    public void testFilter1() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int, z : int);\n";
        pigserver.registerQuery(query);
        query = "B = load " + B 
        		+ " using PigStorage() as (x : int, v : int);\n";
        pigserver.registerQuery(query);
        query = "C = filter A by x%2 == 0;\n";
        pigserver.registerQuery(query);
        query = "D = filter B by x > 2;\n";
        pigserver.registerQuery(query);
        query = "E = join C by x, D by x;\n";
        pigserver.registerQuery(query);
        
        Map<Operator, DataBag> derivedData = pigserver.getExamples2("E");
        assertTrue(derivedData != null);
    }
```

To help you compare Apache Pig's Illustrator with SEDGE, the method PigServer.getExamples() is provided to invoke Apache Pig's Illustrator command.  That is, you can see the example data generated by Apache Pig's Illustrator by replacing *getExamples2* with *getExamples* in the above code.

Compile and run the JUnit test via the commands:
```bash
javac edu/umass/cs/pig/test/TestMotivation.java

java org.junit.runner.JUnitCore edu.umass.cs.pig.test.TestMotivation
```

You will see some output like this:

```bash
-------------------------------------------
| A     | x:int     | y:int    | z:int    | 
-------------------------------------------
|       | 134217728 | 2        | 4        | 
|       | 8181      | 7        | 8        | 
|       | 2         | 2        | 4        | 
-------------------------------------------
-------------------------------------------
| C     | x:int     | y:int    | z:int    | 
-------------------------------------------
|       | 134217728 | 2        | 4        | 
|       | 2         | 2        | 4        | 
-------------------------------------------
--------------------------------
| B     | x:int     | v:int    | 
--------------------------------
|       | 134217728 | 1        | 
|       | 2         | 1        | 
--------------------------------
--------------------------------
| D     | x:int     | v:int    | 
--------------------------------
|       | 134217728 | 1        | 
--------------------------------
-------------------------------------------------------------------------------
| E     | C::x:int    | C::y:int    | C::z:int    | D::x:int    | D::v:int    | 
-------------------------------------------------------------------------------
|       | 134217728   | 2           | 4           | 134217728   | 1           | 
-------------------------------------------------------------------------------
```

To generate example data for a Pig script containing user-defined function (UDF), the UDF needs to be in the classpath.  Also you need to learn how to write Pig's UDF [here](http://pig.apache.org/docs/r0.7.0/udf.html).  The following code shows our JUnit test method [testFilter2](https://docs.google.com/file/d/0B2eUVi06EB0oaURTcjRBTTQxb00/edit?usp=sharing) containing [UDF edu.umass.cs.pig.test.HASH](https://docs.google.com/file/d/0B2eUVi06EB0oS1J3QkhTZVpZX0k/edit?usp=sharing).  *SEDGE* can generates a tuple that can satisfy the condtion x == edu.umass.cs.pig.test.HASH(y) and a tuple that cannot.

```java
@Test
    public void testFilter2() throws Exception {

        PigServer pigserver = new PigServer(pigContext);

        String query = "A = load " + A
                + " using PigStorage() as (x : int, y : int);\n";
        pigserver.registerQuery(query);
        query = "B = FILTER A BY x == edu.umass.cs.pig.test.HASH(y);\n";
        pigserver.registerQuery(query);
        

        Map<Operator, DataBag> derivedData = pigserver.getExamples("B");
        assertTrue(derivedData != null);
    }
```

You will see some output like this:

```bash
-----------------------------
| A     | x:int   | y:int   | 
-----------------------------
|       | 528     | 3       | 
|       | 0       | 3       | 
-----------------------------
-----------------------------
| B     | x:int   | y:int   | 
-----------------------------
|       | 528     | 3       | 
-----------------------------
```

