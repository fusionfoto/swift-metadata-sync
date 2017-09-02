FROM bouncestorage/swift-aio:latest

ENV BUILDER_USERNAME swiftstack
ENV BUILDER_PASSWORD fishface83b
ENV BUILDER_HOST "https://cloudbuilder.swiftstack.com:9025/"

RUN echo "deb http://ppa.launchpad.net/openjdk-r/ppa/ubuntu trusty main" >> /etc/apt/sources.list
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 86F44E2A

RUN apt-get update; apt-get install -y python-support openjdk-8-jdk
RUN apt-get remove -y --purge python-requests python-urllib3

RUN wget --http-user=${BUILDER_USERNAME} --http-password=${BUILDER_PASSWORD} \
    ${BUILDER_HOST}/download/ss-python-urllib3_1.14-5~trusty_all.deb \
    ${BUILDER_HOST}/download/ss-python-elasticsearch_5.4.0-1~trusty_all.deb \
    ${BUILDER_HOST}/download/ss-python-container-crawler_0.0.9-1~trusty_all.deb
RUN dpkg -i ss-python-urllib3_1.14-5~trusty_all.deb \
    ss-python-elasticsearch_5.4.0-1~trusty_all.deb \
    ss-python-container-crawler_0.0.9-1~trusty_all.deb

RUN useradd -M -d /tmp elastic
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.2.tar.gz
RUN tar -xvf elasticsearch-5.5.2.tar.gz && chown -R elastic elasticsearch-5.5.2
RUN echo "network.host: 0.0.0.0" >> /elasticsearch-5.5.2/config/elasticsearch.yml

RUN mkdir /swift-metadata-sync && mkdir /var/lib/swift-metadata-sync
EXPOSE 9200
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'
CMD ["/bin/bash", "/swift-metadata-sync/test/container/launch.sh"]
