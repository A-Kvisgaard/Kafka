# using official python 3.7 image
FROM python:3.8

# declaring work directory
WORKDIR /app
COPY . /app

# installing packages
RUN pip install --upgrade pip
RUN pip install aiokafka
RUN pip install asyncio
RUN pip install opcua

# declaring label
LABEL Name=Archiver Version=0.0.1
ENV KAFKA_BROKER=stage1.cfei.dk:9092
ENV DRIVER_INPUT_TOPIC=opcua.quick.test.1
ENV RETURN_TOPIC=WESLEY_RETURN_TOPIC
ENV GROUP_ID=some.group.id.test.opcua.driver
ENV LDS_URL=opc.tcp://172.17.0.2:48540

VOLUME [ "/app/logs" ]

# running main script
ENTRYPOINT ["python", "archiver.py"]