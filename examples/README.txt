运行:

/opt/cloudera/parcels/CDH/lib/spark/bin/spark-submit \
    --master yarn-client \
    --executor-memory 2g --executor-cores 1 --num-executors 1 \
    --conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
    --jars udps-sdk-0.3-spark1.6.0-cdh5.8.0.jar \
    --class iie.udps.example.spark.OpContainer udps-examples-0.3-spark1.6.0-cdh5.8.0.jar
