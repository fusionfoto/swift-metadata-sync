FROM bouncestorage/swift-aio:latest

RUN apt-get update &&\
    apt-get install --no-install-recommends -y wget openjdk-8-jre-headless git locales &&\
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

RUN pip install -e git://github.com/swiftstack/container-crawler.git@0.0.12#egg=container-crawler && \
    pip install -U coverage nose flake8==3.5.0 mock && \
    pip install -U elasticsearch==5.4.0 && \
    pip uninstall -y hacking

RUN useradd -M -d /tmp elastic
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-5.5.2.tar.gz &&\
    tar -xvf elasticsearch-5.5.2.tar.gz && chown -R elastic elasticsearch-5.5.2

RUN echo "network.host: 0.0.0.0" >> /elasticsearch-5.5.2/config/elasticsearch.yml

RUN mkdir /swift-metadata-sync && mkdir /var/lib/swift-metadata-sync
EXPOSE 9200
RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'
CMD ["/bin/bash", "/swift-metadata-sync/test/container/launch.sh"]
