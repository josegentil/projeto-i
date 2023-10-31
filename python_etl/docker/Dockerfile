FROM python:latest

WORKDIR /app
RUN pip install pandas psycopg2 argparse pymongo 

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . . 

EXPOSE 5000
CMD ["python", "-m", "python_etl\etl.py"]