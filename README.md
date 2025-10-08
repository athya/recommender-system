# recommender-system
Simple recommender system using Kafka (with simulated events), Spark, and Elasticsearch

To start the environment, run $ docker compose up -d --build

To start Kafka producer, run $ docker exec -it python-client python kafka-producer.py

To start spark stream processing, run:

$ docker exec -it spark-controller   spark-submit   --master spark://spark-controller:7077   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:9.1.4 --conf "spark.executor.extraJavaOptions=-Dorg.elasticsearch.hadoop.rest.logging.level=debug" /opt/spark-apps/spark-consumer.py

Set up the Elasticsearch index:

$ curl -X PUT "http://localhost:9200/logs" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "user": {
        "type": "keyword"
      },
      "movie": {
        "type": "keyword"
      },
      "rating": {
        "type": "integer"
      },
      "timestamp": {
        "type": "date"
      }
    }
  }
}'

To search similar movies based on other user activity:

$ docker exec -it python-client python create-recommendation.py

Used this article as a reference for the Kafka + Spark set up: https://medium.com/@yaduvanshineelam09/recommendation-system-using-pyspark-kafka-and-spark-streaming-ba43201ff4bd
