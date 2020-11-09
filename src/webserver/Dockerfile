FROM python:3.7-alpine

# Folder mounting is supposed to be handled by docker-compose

# Install python libs
WORKDIR /usr/src/app
RUN pip install flask

EXPOSE 5000

CMD python3 app.py
