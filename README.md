# Kafka Message Processor

This repository contains three Python scripts for interacting with a Kafka cluster. These scripts allow you to create topics, produce messages to Kafka topics, and consume messages from topics and write them to files.

## Files

1. produce_regions.py: 
   This script reads a CSV file containing Turkish region data and sends each region and its plate code to a Kafka topic named `regions`. Each plate code is used as the key, and the corresponding region is used as the message value.

2. consumer_to_file.py: 
   This script consumes messages from a specified Kafka topic and writes the messages to different files based on the content of the message. For example, messages containing "Iris-setosa", "Iris-versicolor", and "Iris-virginica" are written to separate files.

3. topic_creation.py: 
   This script connects to a Kafka cluster and creates a new topic if it doesn't already exist. The topic can be configured with a custom number of partitions and replication factor.

# Requirements

- Kafka Cluster: You must have a running Kafka cluster.
- Python 3.x: Ensure you have Python 3.x installed.

# To install the required Python libraries, use the following command:
pip install -r requirements.txt

# 1. Create a Kafka Topic
python topic_creation.py

# 2. Produce Messages to Kafka
python produce_regions.py

# Make sure the CSV file path in the script is correct and points to a valid file on your system.

# 3. Consume Messages from Kafka
python consumer_to_file.py --topic your_topic_name
