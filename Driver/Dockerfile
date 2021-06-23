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

ENV ID 1
ENV KAFKA_BROKERS stage1.cfei.dk:9092
ENV INPUT_TOPIC opcua.quick.test.1

# Run the program
ENTRYPOINT ["dotnet", "OPCuakafka.dll"]
