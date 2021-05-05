# Starting image
FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build-env

# Set working directory to /app
WORKDIR /app

# Copy csproj and restore as distinct layers
COPY *.csproj ./

# Install packages
# RUN dotnet add package Newtonsoft.Json --version 12.0.1

# Copy everything else and build
COPY . ./

# Publish the project to a DLL
RUN dotnet publish -c Release -o out

# Build runtime image
FROM mcr.microsoft.com/dotnet/sdk:5.0
WORKDIR /app
COPY --from=build-env /app/out .

ENV W_PORT 8081
ENV W_IP 127.0.0.1
ENV W_KEY sdfsdfsdfsdfsdf
ENV W_SOURCE kafka=stage1.cfei.dk:9092=opcua.quick.test.1
ENV W_SINK kafka=stage1.cfei.dk:9092=softwsdu

# Run the program
# ENTRYPOINT ["dotnet", "run", "EC.MS.csproj"]
ENTRYPOINT ["dotnet", "OPCuakafka.dll"]
