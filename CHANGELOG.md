## 0.0.3 (2017-02-06)

Bug fixes:

    - fix the conversion of last-modified to a proper Elasticsearch date format
      (milliseconds from epoch, as opposed to the mistaken use of seconds)

Improvements:

    - better error messages on failure to index (includes the "reason" from
      Elasticsearch)
