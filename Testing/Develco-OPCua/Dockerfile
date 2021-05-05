# using official python 3.7 image
FROM python:3.7

# declaring work directory
WORKDIR /app
COPY . /app

# installing packages
RUN pip install --upgrade pip
RUN pip install requests
RUN pip install opcua

# declaring label
LABEL Name=OPCUADevelcoServer Version=0.0.3
ENV DEVELCO_URL=http://gw-6b77.sandbox.tek.sdu.dk
ENV PORT=4841
ENV DISCOVERY_SERVER_URL=opc.tcp://10.126.132.36:48540

EXPOSE 4841

VOLUME [ "/app/logs" ]

# running main script
ENTRYPOINT ["python", "Server_Object.py"]