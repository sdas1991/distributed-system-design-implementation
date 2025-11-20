FROM gradle:8.5-jdk17 AS build

WORKDIR /app

# Copy Gradle files
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY gradlew ./

# Copy common libraries
COPY common-libs common-libs/

# Copy ingest service
COPY ingest-service ingest-service/

# Build
RUN gradle :ingest-service:build -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy built JAR
COPY --from=build /app/ingest-service/build/libs/*.jar app.jar

# Expose port
EXPOSE 8081

# Run
ENTRYPOINT ["java", "-jar", "app.jar"]
