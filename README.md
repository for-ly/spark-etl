#spark-etl

spark-submit \
 --class com.corp.car.etl.CarDataEtl \
 --master yarn \
 --deploy-mode cluster \
 --queue root.*** \
 --conf spark.conf.input="hdfs:****" \
 --conf spark.conf.config="hdfs:****/json_config_demo.json" \
 --conf spark.conf.output="hdfs:****" \
 --conf spark.conf.partNum=8 \
 --driver-memory 4g \
 --driver-cores 2 \
 --executor-memory 10g \
 --executor-cores 2 \
 --num-executors 8 \
 /home/q/cardev/car-data-etl-1.0-SNAPSHOT-jar-with-dependencies.jar