## 0.0.6 (2017-05-05)

Improvements:

    - change the handle() to conform to the new API in ContainerCrawler

## 0.0.5 (2017-04-24)

Bug fixes:

    - exit with an error code of 0 and a message if the configuration file does
      not exist. This may happen on a fresh installation of the daemon.

## 0.0.4 (2017-02-28)

Bug fixes:

    - use long for the object size; otherwise, we are limited to reporting
      objects only up to 2GB

## 0.0.3 (2017-02-06)

Bug fixes:

    - fix the conversion of last-modified to a proper Elasticsearch date format
      (milliseconds from epoch, as opposed to the mistaken use of seconds)

Improvements:

    - better error messages on failure to index (includes the "reason" from
      Elasticsearch)
