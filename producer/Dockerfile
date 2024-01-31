FROM python:3.9-slim


# Set the working directory to /app
WORKDIR /app


COPY requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install -r /app/requirements.txt

COPY producer.py /app/producer.py

CMD ["python3", "/app/producer.py"]