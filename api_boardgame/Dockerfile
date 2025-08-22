FROM apache/spark:3.5.6-java17-python3

USER root

RUN wget -P /opt/spark/jars \
    -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.3.0/mysql-connector-j-9.3.0.jar


WORKDIR /home/app

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 5000

WORKDIR /opt/spark/work-dir

CMD python3 /home/app/app.py