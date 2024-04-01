FROM mageai/mageai:latest

ARG PIP=pip3
ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}

# Add Debian Bullseye repository
RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list

# Install OpenJDK 11
RUN apt-get update -y && \
    apt-get install -y openjdk-11-jdk

# Remove Debian Bullseye repository
RUN rm /etc/apt/sources.list.d/bullseye.list

RUN ${PIP} install pyspark

# IF you have python packages to install
#COPY requirements.txt ${USER_CODE_PATH}/requirements.txt 
#RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt

#ENV MAGE_DATA_DIR=