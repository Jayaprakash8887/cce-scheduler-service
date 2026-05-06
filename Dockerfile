# Stage 1: Build
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /app

# Copy Gradle wrapper and build files first for layer caching
COPY gradle/ gradle/
COPY gradlew build.gradle settings.gradle ./
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon 2>/dev/null || true

# Copy source and build
COPY src/ src/
RUN ./gradlew bootJar --no-daemon -x test -x integrationTest

# Stage 2: Runtime
FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S scheduler && adduser -S scheduler -G scheduler

WORKDIR /app

COPY --from=builder /app/build/libs/*.jar app.jar

RUN chown scheduler:scheduler app.jar

USER scheduler

EXPOSE 8083

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8083/actuator/health/liveness || exit 1

ENTRYPOINT ["java", \
  "-XX:+UseZGC", \
  "-XX:MaxRAMPercentage=75.0", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "app.jar"]
