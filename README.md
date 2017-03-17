mvn clean install -Plocal

java -jar SparkVerticle/target/SparkVerticle-0.1-fat.jar -conf src/main/resources/local/app-conf.json -cluster -cluster-host 127.0.0.1

bin/spark-submit --master yarn --name "test" --driver-java-options -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005   --num-executors 1 --executor-cores 1 --conf "spark.executor.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=n,address=wm1b0-8ab.broadinstitute.org:5005,suspend=n" --class com.ddp.SparkVerticle /home/cloudera/workspace/DataGrid/SparkVerticle/target/SparkVerticle-0.1-fat.jar -conf /home/cloudera/workspace/DataGrid/SparkVerticle/src/main/resources/dev/app-conf.json

bin/spark-submit --master yarn --name "test" --class com.ddp.SparkVerticle /home/cloudera/workspace/DataGrid/SparkVerticle/target/SparkVerticle-0.1-fat.jar -conf /home/cloudera/workspace/DataGrid/SparkVerticle/src/main/resources/dev/app-conf.json

