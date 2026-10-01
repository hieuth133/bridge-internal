FROM docker.io/library/eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . .
RUN chmod +x mvnw && ./mvnw -q -DskipTests package dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=target/lib

FROM docker.io/library/eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/target/bridge.jar app.jar
COPY --from=build /src/target/lib lib
CMD ["java","-cp","app.jar:lib/*","org.apache.camel.main.Main"]
