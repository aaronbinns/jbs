The JBs
2011-09-06

The JBs is a Hadoop-based project for processing full-text search
Documents.  The main functions are:

 o Parsing (W)ARC files
 o Merging parsed documents
 o Indexing parsed documents with Lucene or Solr.

Each is described below.

PARSING
=======
To parse (W)ARC files, use the Parse tool, e.g.

 $ hadoop jar jbs.jar org.archive.jbs.Parse <outdir> <(w)arc files...>

The (W)ARC files are named via paths, either local or in HDFS.
The output directory will contain a matching set of files with
names matching the input (W)ARC files.  For example:

 $ hadoop jar jbs.jar org.archive.jbs.Parse parsed/ foo.warc.gz
 $ ls parsed/
 foo.warc.gz

Although the corresponding file in the output directory (parsed/) has
the exact same name as the input (W)ARC file, the output is not a
(W)ARC file, it is a Hadoop sequence file.

The reason for keeping the filenames the same is so that we can easily
track the data from source (W)ARC file to output file.  In fact, the
Parse tool will check that for every input file, if the output file
already exists, then it will skip that input.

INDEXING & MERGING
==================
Both indexing and merging of parsed documents is performed by the same
tool:

  org.archive.jbs.Merge

This tool merges documents and writes them out in various "formats".
The default format is a Hadoop MapFile.  However, a different "format"
can be specified, which writes out the merged documents to either a 
Lucene index or a remote Solr server.

To merge the documents:

 $ hadoop jar jbs.jar org.archive.jbs.Merge <outdir> <inputs...>

To index with Lucene:

 $ hadoop jar jbs.jar org.archive.jbs.Merge -conf conf/index-lucene.xml <outdir> <inputs...>

To index via a remote Solr server:

 $ hadoop jar jbs.jar org.archive.jbs.Merge -conf conf/index-solr.xml <outdir> <inputs...>

Sample configuration files (used above) are provided in the 'conf/'
directory in the JBs.

DEBUGGING
=========
To help trace the flow of data through the system, a simple debug tool is provided which
dumps out the contents of the parsed files:

 $ hadoop jar jbs.jar org.archive.jbs.tools.Dump <input>

where the input can be either a Hadoop MapFile or SequenceFile
produced by the Parse or Merge tools.

MISC
====
There's some other jazz in the misc package, but most of it is
obsolete or early, half-baked experiments.
