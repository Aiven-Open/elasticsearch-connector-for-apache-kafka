FROM eclipse-temurin:11-jdk AS base

ADD ./ connector
WORKDIR connector

RUN ./gradlew installDist

FROM confluentinc/cp-kafka-connect:7.3.3

COPY --from=base /connector/build/install /connector-plugins
