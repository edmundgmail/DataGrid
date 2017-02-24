mvn clean install -Plocal

java -jar SparkVerticle/target/SparkVerticle-0.1-fat.jar -conf src/main/resources/local/app-conf.json -cluster -cluster-host 127.0.0.1
bin/spark-submit --master yarn --name "test" --class io.vertx.core.Launcher /home/cloudera/workspace/ddp/SparkVerticle/target/SparkVerticle-0.1-fat.jar -conf /home/cloudera/workspace/ddp/SparkVerticle/src/main/resources/local/app-conf.json

