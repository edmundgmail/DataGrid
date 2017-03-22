/usr/local/spark2/bin/spark-submit --master yarn --name "test" --class com.ddp.SparkVerticle /home/cloudera/workspace/DataGrid/SparkVerticle/target/SparkVerticle-0.1-fat.jar -Djava.net.preferIPv4Stack=true -cluster -cluster-host 192.168.56.102 
#-conf /home/cloudera/workspace/DataGrid/SparkVerticle/src/main/resources/dev/app-conf.json
