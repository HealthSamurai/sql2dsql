FROM clojure:tools-deps
LABEL authors=""
WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y maven git gcc make build-essential && rm -rf /var/lib/apt/lists/*

# Build libpg_query
RUN git clone --branch 17-latest https://github.com/pganalyze/libpg_query.git /tmp/libpg_query \
    && cd /tmp/libpg_query \
    && make \
    && gcc -shared -fPIC -Wl,--whole-archive libpg_query.a -Wl,--no-whole-archive -o libpg_query.so \
    && cp libpg_query.so /usr/local/lib/ \
    && ldconfig \
    && rm -rf /tmp/libpg_query

# Copy project files
COPY deps.edn .
COPY src/ ./src
COPY resources/ ./resources
COPY java/ ./java

# Build Java code
RUN mvn -f java/pom.xml clean package

# Pre-fetch Clojure dependencies
RUN clojure -P

CMD ["clojure", "-M:run", "/usr/local/lib/libpg_query.so"]