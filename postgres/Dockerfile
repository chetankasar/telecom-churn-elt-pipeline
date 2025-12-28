# ============================================
# PostgreSQL Dockerfile
# ============================================
FROM postgres:15-alpine

# Set environment variables
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=postgres

# Copy initialization scripts
COPY init/ /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD pg_isready -U postgres || exit 1

