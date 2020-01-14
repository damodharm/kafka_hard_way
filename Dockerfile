FROM openjdk:8-jdk-alpine
VOLUME /tmp
RUN ls
RUN ls /var/lib/docker/tmp/docker*/build/libs/*
COPY build/libs/kafka_hard_way-1.0-SNAPSHOT.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
