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
# The data dir must exist and be owned by the runtime user *in the image*: a named volume
# mounted on a non-existent path is created root-owned, and the non-root broker could not
# write its WAL.
RUN mkdir -p /opt/hermes/data && chown -R hermes:hermes /opt/hermes
COPY --from=build --chown=hermes:hermes /src/hermes-rest/target/hermes-rest-*.jar hermes.jar
USER hermes
# Size the heap from the container limit, and exit on OutOfMemoryError so the orchestrator
# restarts a clean process instead of leaving a half-dead JVM running.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError" \
    HERMES_BROKER_ID=1 \
    HERMES_REST_PORT=8081 \
    HERMES_MEMBERS=1@localhost:9091 \
    HERMES_DATA_DIR=/opt/hermes/data
EXPOSE 8081 9091
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s \
  CMD curl -sf http://localhost:${HERMES_REST_PORT}/api/health || exit 1
# Exec form: java is PID 1 and receives SIGTERM, triggering Spring's graceful shutdown.
ENTRYPOINT ["java", "-jar", "hermes.jar"]
