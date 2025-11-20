FROM gradle:8.5-jdk17 AS build

WORKDIR /app

# Copy Gradle files
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY gradlew ./

# Copy common libraries
COPY common-libs common-libs/

# Copy query service
COPY query-service query-service/

# Build
RUN gradle :query-service:build -x test --no-daemon

FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy built JAR
COPY --from=build /app/query-service/build/libs/*.jar app.jar

# Expose port
EXPOSE 8082

# Run
ENTRYPOINT ["java", "-jar", "app.jar"]
