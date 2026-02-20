FROM python:3.10
RUN pip install google-cloud-pubsub
WORKDIR /app
COPY main.py .
CMD ["python", "-u", "main.py"]
