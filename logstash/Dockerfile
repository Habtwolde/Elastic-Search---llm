FROM docker.elastic.co/logstash/logstash:8.14.3

# JDBC plugin is already included in 8.x (do NOT reinstall)
COPY drivers/ojdbc8.jar /usr/share/logstash/drivers/ojdbc8.jar
COPY pipeline/oracle_to_es.conf /usr/share/logstash/pipeline/logstash.conf
