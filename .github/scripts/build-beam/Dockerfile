FROM ubuntu:latest

# Build arguments
ARG GIT_REPO=https://github.com/apache/beam.git
ARG GIT_BRANCH=master

# Install git
RUN apt-get update && apt-get upgrade -y
RUN apt-get install git -y

# Install OpenJDK-11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Install python
RUN apt install python3-venv python3-dev build-essential -y

# Create venv
RUN python3 -m venv ~/.virtualenvs/env

RUN git clone $GIT_REPO

RUN . ~/.virtualenvs/env/bin/activate &&  \
    pip install --upgrade pip setuptools && \
    pip install -e beam/sdks/python[gcp,yaml,test]

RUN apt install golang-go -y

# Checkout given git branch
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
RUN cd beam && git fetch && git checkout $GIT_BRANCH
WORKDIR beam

RUN . ~/.virtualenvs/env/bin/activate && \
#     pip install -e sdks/python[gcp,yaml,test] && \
    python sdks/python/setup.py sdist

RUN . ~/.virtualenvs/env/bin/activate &&  \
    ./gradlew :sdks:python:container:p311:docker

ENTRYPOINT ["/bin/bash"]