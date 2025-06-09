FROM bellsoft/liberica-openjdk-alpine:17

COPY build/libs/*.jar /usr/app/foreverBE.jar

WORKDIR /usr/app

CMD ["java", "-jar", "foreverBE.jar"]
