# syntax=docker/dockerfile:1

FROM python:3.11-slim-buster

# Install git
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the requirements file
COPY requirements.txt requirements.txt

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Copy the application files
COPY ./src/geoserver /app/src/geoserver
COPY ./src/setup.py /app/src/setup.py

RUN pip3 install src/

# Set the command to run the app
CMD [ "python3", "/app/src/geoserver/server.py"]