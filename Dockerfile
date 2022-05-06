# Use the official Rust image.
FROM rust:1.58 as builder

# Copy local code to the container image.
WORKDIR /usr/src/app
COPY . .

# Install production dependencies and build a release artifact.
RUN cargo install --path .

# Import smaller container for runtime
FROM debian:buster-slim as runtime

# RUN apt-get update && apt-get install -y extra-runtime-dependencies && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y libpq-dev ca-certificates

COPY --from=builder /usr/local/cargo/bin/s5table /usr/local/bin/s5table

# Service must listen to $PORT environment variable.
# This default value facilitates local development.
ENV PORT 8080

# Serve Rocket on 0.0.0.0 to bind the host to the container
ENV ROCKET_ADDRESS="0.0.0.0"

# Run the web service on container startup.
CMD ROCKET_PORT=$PORT s5table 


