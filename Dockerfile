# Use the official Apache Airflow image as the base image
FROM apache/airflow:2.8.3

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the Python packages listed in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt



