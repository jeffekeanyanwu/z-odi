# Define targets for automation
.PHONY: all build db-init run run-test clean clean-json test

# Docker image name
IMAGE_NAME=cricket-ingestion-app

# Database file location (in project root)
DB_FILE=odi_data.db

# Full pipeline: build, initialize database, run full ingestion, and cleanup
all: build db-init run clean-json

# Test pipeline: build, initialize database, run test ingestion
test: build db-init run-test

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Initialize the database in project root
db-init:
	docker run --rm -v $(PWD):/app $(IMAGE_NAME) python -m app.db_utils --initialize $(DB_FILE)

# Run the full ingestion process
run:
	docker run --rm -v $(PWD):/app $(IMAGE_NAME) python app/ingest.py

# Run the test ingestion process (with -t flag)
run-test:
	docker run --rm -v $(PWD):/app $(IMAGE_NAME) python app/ingest.py -t

# Clean up JSON files after successful ingestion
clean-json:
	rm -f data/*.json

# Clean up everything (database, logs, and JSON files)
clean:
	rm -f $(DB_FILE)
	rm -f *.log
	rm -rf data/*
