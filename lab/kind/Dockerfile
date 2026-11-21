FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --chown=10001:10001 . /app/
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60.0 -XX:ActiveProcessorCount=2"
ENTRYPOINT ["java", "-jar", "quarkus-run.jar"]
