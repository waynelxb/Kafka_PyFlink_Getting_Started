# Articles. In this tutorial, the port of Kafka is 9098. The port used in docker-compose is 9092.
https://diptimanrc.medium.com/apache-kafka-with-kraft-install-and-configure-on-windows-10-11-no-wsl2-kafka-pyflink-getting-47501f7575d4


# Get images and start containers
prompt> docker-compose up -d

# Create venv
PS D:\Projects\GitHub\Kafka_PyFlink_Getting_Started> python -m  venv venv 
PS D:\Projects\GitHub\Kafka_PyFlink_Getting_Started> ./venv/scripts/activate 
(venv) PS D:\Projects\GitHub\Kafka_PyFlink_Getting_Started> pip install -q apache-flink confluent-kafka kafka-python

# Create requirement.txt
(venv) PS D:\Projects\GitHub\Kafka_PyFlink_Getting_Started> pip freeze > requirements.txt



