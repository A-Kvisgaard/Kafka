# Develco-restful-python

Develco-restful-python exposes a Develco API as an OPC-UA server.

## Installation

Uses requests & opcua
```bash
pip install requests
pip install opcua

```

## Usage
Change Environmental variables in dockerfile as needed

```
ENV DEVELCO_URL=http://gw-2ab0.sandbox.tek.sdu.dk
ENV PORT=4841
ENV DISCOVERY_SERVER_URL=opc.tcp://10.126.132.36:48540
```
