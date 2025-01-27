FROM python:3.12.8

# Install necessary packages
RUN pip install pandas sqlalchemy psycopg2-binary

# Set working directory in the container
WORKDIR /app

# Copy your Python script into the container
COPY ingest_data.py ingest_data.py

# Specify the entry point to run the Python script
ENTRYPOINT [ "python", "ingest_data.py" ]
