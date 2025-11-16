# Streaming Service QoE Analytics - PostgreSQL Data Warehouse
# This Dockerfile creates a PostgreSQL container with our schema pre-loaded

FROM postgres:16-alpine

# Set metadata
LABEL maintainer="sada@rayfieldsquare.com"
LABEL description="Streaming Service QoE Analytics Data Warehouse"
LABEL version="1.0"

# Environment variables for PostgreSQL
# These can be overridden in docker-compose.yaml
ENV POSTGRES_DB=streaming_analytics
ENV POSTGRES_USER=analytics_user
ENV POSTGRES_PASSWORD=analytics_password

# Create directory for initialization scripts
# PostgreSQL automatically runs *.sql and *.sh files in /docker-entrypoint-initdb.d/
# in alphabetical order when the container is first created
RUN mkdir -p /docker-entrypoint-initdb.d/

# Copy SQL schema and initialization scripts
# These will run automatically when the database is first created
COPY schema_design.sql /docker-entrypoint-initdb.d/01-schema.sql
# COPY create_aggregates.sql /docker-entrypoint-initdb.d/02-aggregates.sql
# COPY data_quality_checks.sql /docker-entrypoint-initdb.d/03-quality-checks.sql

# Copy custom PostgreSQL configuration (optional - for performance tuning)
# COPY postgresql.conf /etc/postgresql/postgresql.conf

# Expose PostgreSQL port
EXPOSE 5432

# Health check to ensure database is ready
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U analytics_user -d streaming_analytics || exit 1

# The base postgres image already has the correct ENTRYPOINT and CMD
# No need to override them