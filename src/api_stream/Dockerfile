FROM python:3.7-alpine

# Init bash
RUN apk add --no-cache bash

# Install Python libs
RUN pip install requests
RUN pip install kafka-python

# Copy Python file and execute
ADD . /usr/src/app/
WORKDIR /usr/src/app

# Start streaming application
# CMD tail -f /dev/null
CMD ./wait-for-it.sh localhost:9092 -t 0 -- python3 open_weather_api.py
