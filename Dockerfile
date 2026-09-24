# Hermes broker node: Maven build stage + slim JRE runtime.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY hermes-core/pom.xml hermes-core/
COPY hermes-rest/pom.xml hermes-rest/
RUN mvn -B -q dependency:go-offline
COPY hermes-core/src hermes-core/src
COPY hermes-rest/src hermes-rest/src
RUN mvn -B -q clean package -DskipTests

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /opt/hermes hermes
WORKDIR /opt/hermes
COPY --from=build /src/hermes-rest/target/hermes-rest-*.jar hermes.jar
USER hermes
ENV HERMES_BROKER_ID=1 \
    HERMES_REST_PORT=8081 \
    HERMES_MEMBERS=1@localhost:9091 \
    HERMES_DATA_DIR=/opt/hermes/data
EXPOSE 8081 9091
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s \
  CMD curl -sf http://localhost:${HERMES_REST_PORT}/api/health || exit 1
ENTRYPOINT ["java", "-jar", "hermes.jar"]
