
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.lucene.store.*;
import org.apache.lucene.document.*;

/**
 * Code hacked out of Nutch that can be used in a MapReduce program to
 * index documents into a ${temp} Lucene directory, then copy the
 * completed index into the part-nnnnn directory when the reduce is
 * completed.  This approach works fine when the indexing is done as
 * part of the Reduce phase, but I'm not sure it would work just as
 * well if used for the Map.
 */
public class LuceneOutputFormat extends FileOutputFormat<Text, MapWritable>
{
  public FileSystem fs;

  public Path temp;
  public Path perm;
  
  public IndexWriter writer;
  
  public RecordWriter<Text, MapWritable> getRecordWriter( FileSystem notUsed,
                                                          JobConf job,
                                                          String name,
                                                          Progressable progress )
    throws IOException
  {
    // Open Lucene index in ${temp}
    this.fs = FileSystem.get(job);

    perm = new Path(FileOutputFormat.getOutputPath(job), name);
    temp = job.getLocalPath("index/_"  + (new Random().nextInt()));

    fs.delete(perm, true); // delete old, if any
    writer = new IndexWriter( new NIOFSDirectory( new File( fs.startLocalOutput( perm, temp ).toString( ) ) ),
                              new org.apache.lucene.analysis.standard.StandardAnalyzer( Version.LUCENE_CURRENT ),
                              IndexWriter.MaxFieldLength.UNLIMITED );

    /*
    writer.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
    writer.setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
    writer.setMaxMergeDocs(job.getInt("indexer.maxMergeDocs", Integer.MAX_VALUE));
    writer.setTermIndexInterval(job.getInt("indexer.termIndexInterval", 128));
    writer.setMaxFieldLength(job.getInt("indexer.max.tokens", 10000));
    writer.setInfoStream(LogUtil.getDebugStream(Indexer.LOG));
    writer.setUseCompoundFile(false);
    writer.setSimilarity(new NutchSimilarity());
    */
    
    LuceneRecordWriter w = new LuceneRecordWriter( );

    return w;
  }

  public class LuceneRecordWriter implements RecordWriter<Text, MapWritable>
  {
    public void write( Text key, MapWritable map )
      throws IOException
    {
      // TODO: Put the document into the Lucene index.
      Document doc = new Document();
      
      MapAdapter a = new MapAdapter( map );

      // Only do text/html for now.
      if ( ! "text/html".equals( a.get( "type" ) ) )
        {
          return ;
        }

      doc.add( new Field( "url",        a.get("url"),            Field.Store.YES,      Field.Index.NO ) );
      doc.add( new Field( "title",      a.get("title"),          Field.Store.YES,      Field.Index.ANALYZED) );
      doc.add( new Field( "content",    a.get("content_parsed"), Field.Store.COMPRESS, Field.Index.ANALYZED) );
      doc.add( new Field( "collection", a.get("collection"),     Field.Store.YES,      Field.Index.NOT_ANALYZED_NO_NORMS) );
      doc.add( new Field( "type",       a.get("type"),           Field.Store.YES,      Field.Index.NOT_ANALYZED_NO_NORMS) );
      doc.add( new Field( "length",     a.get("length"),         Field.Store.YES,      Field.Index.NO) );
      doc.add( new Field( "date",       a.get("date"),           Field.Store.YES,      Field.Index.NO) );
      
      String url = a.get( "url" );
      try
        {
          String site = (new URL( url)).getHost( );

          // Strip off any "www." header.
          if ( site.startsWith( "www." ) )
            {
              site = site.substring( 4 );
            }

          doc.add( new Field( "site", site, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS) );

        }
      catch ( MalformedURLException mue )
        {
          // Rut-roh.
        }
      
      writer.addDocument( doc );
    }

    
    public void close( Reporter reporter )
      throws IOException
    {
      // Optimize and close the IndexWriter
      writer.optimize();
      writer.close();

      // Copy the index from ${temp} to HDFS and touch a "done" file.
      fs.completeLocalOutput(perm, temp);
      fs.createNewFile(new Path(perm, "done"));
    }
  }
  
}


class MapAdapter
{
  MapWritable mw;
  Text textKey;

  public MapAdapter( MapWritable mw )
  {
    this.mw = mw;
    this.textKey = new Text();
  }

  public String get( String key )
  {
    this.textKey.set( key );

    Text value = (Text) this.mw.get( this.textKey );

    if ( value == null ) return "";

    return value.toString();
  }
}
