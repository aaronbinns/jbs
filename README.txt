The JBs
2011-09-06

The JBs is a Hadoop-based project for processing full-text search
Documents.  The main functions are merging documents and indexing
documents into either Lucene or Solr back-ends.

The main tool is:

  org.archive.jbs.Merger

which merged Documents, as well as writes them to various
destinations.  By default, the merged documents are written to a
Hadoop MapFile; but by selecting a Lucene or Solr destination, the
documents are indexed using those backends.

There is also a tool to count the unique inlinks to documents:

  org.archive.jbs.PageRank*

*it should probably be renamed since it doesn't do PageRank.  It just
 counts inlinks.

There's some other jazz in the misc package, but most of it is
obsolete or early, half-baked experiments.

