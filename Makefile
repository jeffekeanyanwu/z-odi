# Define targets for automation
.PHONY: all build db-init run clean

# Docker image name
IMAGE_NAME=cricket-ingestion-app

# Database file location (in project root)
DB_FILE=odi_data.db

# Full pipeline: build, initialize database, and run ingestion
all: build db-init run

# Build the Docker image
build:
	docker build -t $(IMAGE_NAME) .

# Initialize the database in project root
db-init:
	docker run --rm -v $(PWD):/app $(IMAGE_NAME) python -m app.db_utils --initialize $(DB_FILE)

# Run the ingestion process
run:
	docker run --rm -v $(PWD):/app $(IMAGE_NAME) python app/ingest.py

# Clean up data and temporary files
clean:
	rm -f $(DB_FILE)
	rm -f *.log
	rm -rf data/*
