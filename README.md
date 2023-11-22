# Spark Streaming Project

## Project Overview

	This Spark Streaming Project is designed to consume data from a Kafka topic, perform data validation and transformations, and finally store the processed data into HDFS. 
	The project structure is organized to support modular development, testing, and deployment.

## Directory Structure

	spark_streaming_project/
	│
	├── src/                          # Source files
	│   ├── main.py                   # Main application script
	│   ├── kafka_streaming.py        # Kafka streaming module
	│   ├── hdfs_operations.py        # HDFS operations module
	│   └── data_validation.py        # Data validation module
	│
	├── tests/                        # Unit tests
	│   ├── test_kafka_streaming.py
	│   ├── test_hdfs_operations.py
	│   └── test_data_validation.py
	│
	├── config/                       # Configuration files
	│   └── config.yaml
	│
	├── logs/                         # Log files
	│   └── application.log
	│
	├── data/                         # Sample data
	│   └── sample.xml
	│
	├── scripts/                      # Deployment scripts
	│   └── deploy.sh
	│
	├── README.md                     # Project documentation
	└── requirements.txt              # Project dependencies

## Setup and Installation

	pip install -r requirements.txt

## Running the Application

	Execute the main application script: spark-submit src/main.py
	
	Alternatively, use the deployment script: sh scripts/deploy.sh

## Testing

	pytest tests/


## Modules Description

	main.py: Orchestrates the data processing pipeline.

	kafka_streaming.py: Handles consuming data from Kafka.

	hdfs_operations.py: Manages read and write operations to HDFS.

	data_validation.py: Performs validation and formatting on the data.

## Configuration

	The config.yaml file contains essential configurations for Kafka and HDFS. Update it according to your cluster settings.

## Logs

	Application logs are stored in the logs/ directory. Check application.log for runtime logs.

## Data

	Sample data for testing and development purposes can be found in the data/ directory.

## Deployment

	Use scripts/deploy.sh to deploy the application in your Spark environment.