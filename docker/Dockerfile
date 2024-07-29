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
RUN pip install -r requirements.txt

# Copy the application files
COPY ./src /app/src

ENV PYTHONPATH="${PYTHONPATH}:/app/src"
ENV PORT=8000
ENV HOST="localhost"

# Set the command to run the app
CMD [ "python3", "/app/src/server.py"]