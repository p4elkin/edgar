# cache as most as possible in this multistage dockerfile.
FROM maven:3.6-alpine as DEPS

WORKDIR /opt/app

COPY edgar-commons/pom.xml edgar-commons/pom.xml
COPY edgar-entities/pom.xml edgar-entities/pom.xml
COPY edgar-webapp/pom.xml edgar-webapp/pom.xml
COPY edgar-data-collector/pom.xml edgar-data-collector/pom.xml
COPY pom.xml pom.xml

RUN mvn -B -e -C org.apache.maven.plugins:maven-dependency-plugin:3.1.2:go-offline

FROM maven:3.6.3-openjdk-8-slim AS build

WORKDIR /opt/app
COPY --from=deps /root/.m2 /root/.m2
COPY --from=deps /opt/app/ /opt/app

COPY edgar-commons/src edgar-commons/src
COPY edgar-entities/src edgar-entities/src
COPY edgar-webapp/src edgar-webapp/src
COPY edgar-data-collector/src edgar-data-collector/src

RUN mvn -B -e clean install -DskipTests=true

FROM openjdk:14-jdk-alpine
RUN apk add --no-cache bash
COPY --from=build /opt/app/edgar-webapp/target/edgar-webapp-1.0-SNAPSHOT.jar ./app.jar
ENV address mongodb://mongo
ENTRYPOINT ["/bin/bash", "-c", "java -jar app.jar -Ddb.address=$address -Dspring.profiles.active=production"]
