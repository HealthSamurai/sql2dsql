FROM clojure:tools-deps

LABEL authors=""
WORKDIR /app

# Install Maven
RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY deps.edn .
COPY src/ ./src
COPY resources/ ./resources
COPY java/ ./java

# Build Java code (optional if you have Java sources)
RUN mvn -f java/pom.xml clean package

# Pre-fetch Clojure dependencies
RUN clojure -P

CMD ["clojure", "-M:run"]
