# Large-Scale-Data-Management-Systems

## Transform CSVs to Parquets

hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/create_parquets.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name create-parquets-job \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/create_parquets.py

## Query 1

#### DataFrame API no UDF

hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_1/q1_df_no_udf.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q1-df-no-udf \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q1_df_no_udf.py

#### Dataframe API with UDF
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_1/q1_df_udf.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q1-df-with-udf \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q1_df_udf.py

#### RDD with UDF
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_1/q1_rdd_udf.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q1-rdd-with-udf \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q1_rdd_udf.py

#### RDD no UDF
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_1/q1_rdd_no_udf.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q1-rdd-no-udf \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q1_rdd_no_udf.py

## Query 2

#### DataFrame API
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query2/q2_df.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q2-df \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q2_df.py

#### SQL API
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query2/q2_sql.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q2-sql \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q2_sql.py

#### RDD
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query2/q2_rdd.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q2-rdd \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q2_rdd.py

## Query 3

#### DataFrame API

hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_3/q3_df_both.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q3-df \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q3_df_both.py

#### RDD

hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_3/q3_rdd_both.py

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q3-rdd \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q3_rdd_both.py


## Query 4
hdfs dfs -put -f /home/alex/Desktop/BigDataAssignment/Big-Data-Analysis/query_4/q4_df.py

#### 2 Executors × 4 cores / 8 GB memory

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q4-df-2exec-4cores-8gb \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=8g \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q4_df.py

#### 4 Executors × 2 cores / 4 GB memory

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q4-df-4exec-2cores-4gb \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=4 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=4g \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q4_df.py


#### 8 Executors × 1 core / 2 GB memory

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q4-df-8exec-1cores-2gb \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=8 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q4_df.py

#### 2 Executors × 1 core / 2 GB memory

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q4-df-2exec-1cores-2gb \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q4_df.py

#### 2 Executors × 2 cores / 4 GB memory

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q4-df-2exec-2cores-4gb \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=4g \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q4_df.py

#### 2 Executors × 4 cores / 8 GB memory

spark-submit \
    --master k8s://https://termi7.cslab.ece.ntua.gr:6443 \
    --deploy-mode cluster \
    --name q4-df-2exec-4cores-8gb \
    --conf spark.hadoop.fs.permissions.umask-mode=000 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.namespace=alexiosfilippakopoulos-priv \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=8g \
    --conf spark.kubernetes.container.image=apache/spark \
    --conf spark.kubernetes.submission.waitAppCompletion=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    --conf spark.history.fs.logDirectory=hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/logs \
    hdfs://hdfs-namenode:9000/user/alexiosfilippakopoulos/q4_df.py


