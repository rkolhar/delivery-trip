FROM python:3.9-slim

# Set the working directory to /app
WORKDIR /app

COPY requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install -r /app/requirements.txt

COPY consumer.py /app/consumer.py

CMD ["python3", "/app/consumer.py"]