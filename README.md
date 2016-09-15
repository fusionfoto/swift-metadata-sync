Swift metadata sync
===================

Synchronize Swift metadata with an Elasticsearch index. The tool can be used
through the `swift-metadata-sync` binary. The synchronization daemon needs to
run on a Swift node that hosts a container database.

The daemon will attempt to ensure correct mappings for the fields it knows about
(e.g. x-timestamp, last-modified, content-length, etag). However, if an
incorrect mapping has already been created, the metadata will still be
propagated, but may not be searchable (e.g. a range query against dates would
not return expected results).

Usage
-----

The bare minimum required is a configuration file. It must include a pointer to
the Swift drives (`devices`), the status directory to use (`status_dir`), number
of items to process at a time (`items_chunk`), and an array of container mappings.

Here is a sample configuration file:

	{
		"devices": "/srv/node",
		"status_dir": "/tmp",
		"items_chunk": 1000,
		"containers": [
			{
				"account": "AUTH_swift",
				"container": "swift",
				"es_hosts": "192.168.22.1",
				"index": "stuff"
			}
		]
	}


For each Swift Account/Container, an elasticsearch cluster (`es_hosts`) and
index (`index`) must be specified. The hosts argument accepts multiple,
comma-separated entries to specify numerous servers.

If an index is changed and a re-index is desired, changing a container mapping's
`index` value will restart indexing from the first object in that container.

Design
------

The daemon walks the Swift container database present on the node. The database
rows contain the names of the objects and their status (notably last modified
date and whether the object has been deleted). When an object's metadata is
mutated, as long as fast-POST is enabled, a new row will be inserted in the
database and the prior entry removed. This allows the daemon to continually walk
the databases rows forward.

The advantage of this approach is that we never have to scan the entire
database. The daemons must run on each of the container nodes to ensure that
metadata is propagated, even if some of the container nodes fail.

There is no coordination mechanism between the processes walking the database,
but each one attempts to only process a fraction of the entries and then
verifies that all entries have been propagated.

After a failure, a daemon can be safely replaced with another node. It will only
be doing bulk queries against Elasticsearch to verify that the changes it
expects to have observed have been made. Once it catches up, it will resume
operation from where the failure occurred.

The daemons also record the database ID. If a drive fails and the database has
to replicated from another Swift node, the daemon will also restart from the
beginning. This ensures correctness, but means that updates may not propagate as
quickly after drive failures.
