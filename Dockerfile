FROM python:3.11-slim

WORKDIR /app

# Copy the specific app folder
COPY app /app/app

# Set PYTHONPATH so python can find your modules inside /app
ENV PYTHONPATH=/app

# Expose Redis port
EXPOSE 6379

# Create volume for your AOF/RDB persistence
VOLUME ["/data"]

# Run the server
CMD ["python3", "app/main.py", "--port", "6379"]