FROM python:3.10-slim
WORKDIR /app

# Install MongoDB tools for backups
RUN apt-get update && apt-get install -y mongodb-clients && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD python3 main.py
