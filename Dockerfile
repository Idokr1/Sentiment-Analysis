FROM openjdk:11
COPY target/sentiment*.jar /usr/src/sentiment.jar
COPY src/main/resources/application.properties /opt/conf/application.properties
CMD ["java", "-jar", "/usr/src/sentiment.jar", "--spring.config.location=file:/opt/conf/application.properties"]