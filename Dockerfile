# using python 3.10 as base image
FROM python:3.10

# setting working directory
WORKDIR /app/

# copying and installing packages
COPY app/requirements.txt .
RUN python -m pip install --upgrade pip==25.0.1
RUN python -m venv venv
RUN pip install --no-cache-dir -r requirements.txt

# return to root and create data directory
WORKDIR /
RUN mkdir /data
COPY data/ /data/

# copy source code
WORKDIR /app/
COPY app/ .