# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
# This is the crucial step that includes gcp_utils.py
COPY . .

# Command to run your main script when the container starts
CMD ["python", "main.py"]