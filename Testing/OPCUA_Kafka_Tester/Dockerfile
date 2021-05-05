FROM python:3.7.4

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip
#RUN pip install opcua
RUN pip install asyncio
RUN pip install aiokafka

#EXPOSE 4800

#ENTRYPOINT ["python", "simple-server.py"]
ENTRYPOINT ["python", "-u", "test_app.py"]
