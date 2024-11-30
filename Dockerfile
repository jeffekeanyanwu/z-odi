FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements and install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Create data directory
RUN mkdir -p /app/data

# Copy the entire application code into the container
COPY . /app

# Set PYTHONPATH to include /app
ENV PYTHONPATH=/app

# Default command to run the application
CMD ["python", "app/ingest.py"]
