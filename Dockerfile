FROM openjdk:8-jdk-alpine
VOLUME /tmp
CMD ["ls"]
COPY build/libs/kafka_hard_way-1.0-SNAPSHOT.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
