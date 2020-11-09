FROM openjdk:8-jre-slim
COPY --from=python:3.7 / /

# Install Python libs
RUN pip install numpy scipy matplotlib seaborn pandas 
RUN pip install pyspark==2.4.4
RUN pip install cassandra-driver

# Setup env variables
ENV SPARK_HOME /usr/local/lib/python3.7/site-packages/pyspark

# Folder mounts are supposed to be handled by docker-compose
WORKDIR /usr/src/batch_processing

# Start batch processing application
CMD ./wait-for-it.sh localhost:9042 -t 0 -- python batch_processing.py
