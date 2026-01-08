FROM apache/spark-py:latest

# Set working directory
WORKDIR /opt/spark

# Fix permissions for entrypoint script and create work directories
USER root
RUN mkdir -p /opt/spark/work-dir /tmp/spark-worker && \
    chmod 777 /opt/spark/work-dir /tmp/spark-worker && \
    touch /opt/spark/work-dir/java_opts.txt && \
    chmod 666 /opt/spark/work-dir/java_opts.txt || true
USER 185

# Expose Spark ports
EXPOSE 8080 7077 6066

# Default command (can be overridden in docker-compose)
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
