#  EV_Central Dockerfile - builds container imgae
FROM python:3.11-slim

WORKDIR /app
COPY ../common /app/common
COPY EV_Central.py central_db.py /app/
COPY ../common/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 9002
CMD ["python", "EV_Central.py"]
