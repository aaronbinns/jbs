
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

  public Set<String> allowedContentTypes = new HashSet<String>( );
  public Map<String,String> contentTypeAliases = new HashMap<String,String>( );
  {
    // PDF aliases.
    contentTypeAliases.put( "application/x-pdf", "application/pdf" );

    // HTML aliases.
    contentTypeAliases.put( "application/xhtml+xml", "text/html" );

    // MS Word aliases.
    contentTypeAliases.put( "application/vnd.ms-word", "application/msword" );
    contentTypeAliases.put( "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "application/msword" );

    // PowerPoint aliases.
    contentTypeAliases.put( "application/mspowerpoint",     "application/vnd.ms-powerpoint" );
    contentTypeAliases.put( "application/ms-powerpoint",    "application/vnd.ms-powerpoint" );
    contentTypeAliases.put( "application/mspowerpnt",       "application/vnd.ms-powerpoint" );
    contentTypeAliases.put( "application/vnd-mspowerpoint", "application/vnd.ms-powerpoint" );
    contentTypeAliases.put( "application/powerpoint",       "application/vnd.ms-powerpoint" );
    contentTypeAliases.put( "application/x-powerpoint",     "application/vnd.ms-powerpoint" );
    contentTypeAliases.put( "application/vnd.openxmlformats-officedocument.presentationml.presentation", "application/vnd.ms-powerpoint" );
  }
  
  public RecordWriter<Text, MapWritable> getRecordWriter( FileSystem notUsed,
                                                          JobConf job,
                                                          String name,
                                                          Progressable progress )
    throws IOException
  {
    // Populate our list of allowed Content-Types
    String types = job.get( "indexer.allowedContentTypes", "application/msword application/pdf application/vnd.ms-powerpoint text/html text/plain" );

    for ( String type : types.split( "\\s+" ) )
      {
        if ( type.length() < 1 ) continue;

        allowedContentTypes.add( type );
      }

    // Open Lucene index in ${temp}
    this.fs = FileSystem.get(job);

    perm = new Path(FileOutputFormat.getOutputPath(job), name);
    temp = job.getLocalPath( "index/_"  + (new Random().nextInt()) );

    fs.delete(perm, true); // delete old, if any
    writer = new IndexWriter( new NIOFSDirectory( new File( fs.startLocalOutput( perm, temp ).toString( ) ) ),
                              new org.apache.lucene.analysis.standard.StandardAnalyzer( Version.LUCENE_CURRENT ),
                              IndexWriter.MaxFieldLength.UNLIMITED );

    /*
    writer.setMergeFactor(job.getInt("indexer.mergeFactor", 10));
    writer.setMaxBufferedDocs(job.getInt("indexer.minMergeDocs", 100));
    writer.setMaxMergeDocs(job.getInt("indexer.maxMergeDocs", Integer.MAX_VALUE));
    writer.setTermIndexInterval(job.getInt("indexer.termIndexInterval", 128));
    writer.setSimilarity(new NutchSimilarity());
    */

    writer.setMaxFieldLength( job.getInt("indexer.max.tokens", Integer.MAX_VALUE) );
    writer.setUseCompoundFile(false);
    
    LuceneRecordWriter w = new LuceneRecordWriter( );

    return w;
  }

  public class LuceneRecordWriter implements RecordWriter<Text, MapWritable>
  {
    public void write( Text key, MapWritable map )
      throws IOException
    {
      Document doc = new Document();
      
      MapAdapter a = new MapAdapter( map );

      // Skip if not an allowed type.
      String type = a.get( "type" );

      // First, canonicalize the type name.
      if ( contentTypeAliases.containsKey( type ) )
        {
          type = contentTypeAliases.get( type );
        }

      // If there are any explicitly allowed content-types, then this
      // has to be one of them.
      if ( allowedContentTypes.size() > 0 && ! allowedContentTypes.contains( type ) )
        {
          return ;
        }

      // Skip if text/plain and /robots.txt
      try
        {
          URI uri = new URI( a.get("url") );
          if ( "text/plain".equals( type ) && "/robots.txt".equals( uri.getPath() ) )
            {
              return ;
            }
        } catch ( URISyntaxException e ) { }

      // TODO: Some heuristic to detect, and skip, bogus text/plain files.  Like video/flash/etc.
      //       mis-labeled as text/plain.

      // Special handling for type.  Index on the canonicalized type, but store the original type.
      /*
      doc.add( new Field( "type", type,          Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
      doc.add( new Field( "type", a.get("type"), Field.Store.YES, Field.Index.NO ) );
      */
      // Nahh, just use the canonicalized type for both.
      doc.add( new Field( "type", type, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
      
      doc.add( new Field( "url",        a.get("url"),        Field.Store.YES, Field.Index.NO                    ) );
      doc.add( new Field( "title",      a.get("title"),      Field.Store.YES, Field.Index.ANALYZED              ) );
      doc.add( new Field( "collection", a.get("collection"), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
      doc.add( new Field( "length",     a.get("length"),     Field.Store.YES, Field.Index.NO                    ) );

      // Special handling for content field.
      String content = a.get( "content_parsed" );
      if ( content.length() > 0 )
        {
          doc.add( new Field( "content", content, Field.Store.NO, Field.Index.ANALYZED ) );

          byte[] compressed = CompressionTools.compressString( content );

          // Store the shorter of the two.
          if ( compressed.length < content.length() )
            {
              doc.add( new Field( "content", compressed, Field.Store.YES ) );    
            }
          else
            {
              doc.add( new Field( "content", content, Field.Store.YES, Field.Index.NO ) );
            }
        }

      // Special handling for dates.
      String date = a.get("date");
      if ( date.length() == "yyyymmddhhmmss".length( ) )
        {
          // Store, but do not index, the full 14-character date.
          doc.add( new Field( "date", date,                   Field.Store.YES, Field.Index.NO  ) );

          // Index, but do not store, the year and the year+month.  These are what can be searched.
          doc.add( new Field( "date", date.substring( 0, 4 ), Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
          doc.add( new Field( "date", date.substring( 0, 6 ), Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
        }

      // Special handling for site
      String url = a.get( "url" );
      try
        {
          String site = (new URL( url)).getHost( );

          // Strip off any "www." header.
          if ( site.startsWith( "www." ) )
            {
              site = site.substring( 4 );
            }

          doc.add( new Field( "site", site, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS) );
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

    return value.toString().trim();
  }
}
