FROM openjdk:8-jre-slim

# Environment
ENV SBT_VERSION 1.3.13

# Install SBT
RUN apt-get update && apt-get install -y curl
RUN curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
    rm sbt-$SBT_VERSION.deb && \
    apt-get update && \
    apt-get install sbt

# Compile application
COPY StreamProcessing.scala /usr/src/app/StreamProcessing.scala
COPY build.sbt /usr/src/app/build.sbt
COPY wait-for-it.sh /usr/src/app/wait-for-it.sh
WORKDIR /usr/src/app
RUN chmod +x wait-for-it.sh

RUN sbt compile
# Start streaming application
CMD ./wait-for-it.sh localhost:9042 -t 0 -- sbt run
