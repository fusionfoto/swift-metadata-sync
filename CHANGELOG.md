## 0.0.9 (2017-07-13)

Bug fixes:

    - change the constructor to work with the changes to the Container Crawler.

## 0.0.8 (2017-07-12)

Bug fixes:

    - properly handle unicode characters in object names.

## 0.0.7 (2017-06-12)

Improvement:

    - do not rely on the deprecated "found" field in DELETE responses.
    - use "text" or "keyword" fields when creating mappings for Swift objects
      (as opposed to "string"). We will now check the Elasticsearch version and
      only use "string" with Elasticsearch servers < 5.x.
    - bump the Elasticsearch library requirement to 5.x.

Bug fixes:

    - change the document ID to be SHA256 of account, container, and object. The
      string is concatenated with the "/" separator. This will ensures that we
      can work with long object names and Elasticsearch version 5.0 or newer,
      which reject IDs longer than 512 characters.

## 0.0.6 (2017-05-05)

Improvements:

    - change the handle() to conform to the new API in ContainerCrawler.

## 0.0.5 (2017-04-24)

Bug fixes:

    - exit with an error code of 0 and a message if the configuration file does
      not exist. This may happen on a fresh installation of the daemon.

## 0.0.4 (2017-02-28)

Bug fixes:

    - use long for the object size; otherwise, we are limited to reporting
      objects only up to 2GB.

## 0.0.3 (2017-02-06)

Bug fixes:

    - fix the conversion of last-modified to a proper Elasticsearch date format
      (milliseconds from epoch, as opposed to the mistaken use of seconds).

Improvements:

    - better error messages on failure to index (includes the "reason" from
      Elasticsearch).
