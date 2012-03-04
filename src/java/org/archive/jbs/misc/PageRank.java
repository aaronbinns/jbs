/*
 * Copyright 2011 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.archive.jbs.misc;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.metadata.Metadata;

import org.archive.jbs.util.*;
import org.archive.jbs.Document;

/**
 * <p>
 *   Map/Reduce job to produce "page rank" information.  In this case,
 *   we are merely counting the number of links <em>to</em> a URL that
 *   is in the collection.
 * </p>
 * <p>
 *   Since the URLs in an archival collection are keyed by a
 *   combination of URL and digest, yet the outlinks on a page only
 *   have a URL (no digest of the linked-to page), we apply links to a
 *   URL to all versions of that URL.
 * </p>
 * <p>
 *   This map/reduce job performs both tasks: counting the number of
 *   inlinks, and matching the URLs to the URL+hash keys of the pages
 *   in the collection.  This means that an <em>entire</em> collection
 *   must be page-ranked as one job.
 * </p>
 * <p>
 *   The output is a Hadoop SequenceFile with a Text key and a
 *   JSON-encoded Document as the value.  The Document has a
 *   property of "numInlinks" with corresponding value.
 * </p>
 */
public class PageRank extends Configured implements Tool
{
  public static final Log LOG = LogFactory.getLog(PageRank.class);

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, GenericObject>
  {
    // For efficiency, create one instance of the output object '1'.
    private static final GenericObject ONE = new GenericObject( new LongWritable( 1 ) );

    private boolean   ignoreInternalLinks = true;
    private IDNHelper idnHelper;

    /**
     * Configure the job by obtaining local copy of relevant
     * properties as well as building the IDNHelper which is used for
     * getting the host/domain of a URL.
     */
    public void configure( JobConf job )
    {
      this.ignoreInternalLinks = job.getBoolean( "pageranker.ignoreInternalLinks", true );

      try
        {
          this.idnHelper = buildIDNHelper( job );
        }
      catch ( IOException ioe )
        {
          throw new RuntimeException( ioe );
        }
    }
    
    /**
     * Map the document's outlinks to inlinks for the URL being
     * linked-to.  Skip intra-domain links, or any that are malformed.
     * Also emit the source URL with it's digest as the payload to the
     * linked-to URLs can be joined with the list of captured URLs.
     */
    public void map( Text key, Writable value, OutputCollector<Text, GenericObject> output, Reporter reporter)
      throws IOException
    {
      String[] keyParts = key.toString().split("\\s+");

      // Maformed key, should be "url digest".  Skip it.
      if ( keyParts.length != 2 ) return;

      String fromUrl    = keyParts[0];
      String fromDigest = keyParts[1];
      String fromHost   = getHost( fromUrl );

      // If there is no fromHost, skip it.
      if ( fromHost == null || fromHost.length() == 0 ) return;

      // Emit the source URL with it's digest as the payload.
      output.collect( new Text( fromUrl ), new GenericObject( new Text( fromDigest ) ) );

      // Now, get the outlinks and emit records for them.
      Set<String> uniqueOutlinks = null;
      if ( value instanceof ParseData )
        {
          uniqueOutlinks = getOutlinks( (ParseData) value );
        }
      else if ( value instanceof Text )
        {
          uniqueOutlinks = getOutlinks( new Document( ((Text) value).toString() ) );
        }
      else 
        {
          // Hrmm...what type could it be...
          return ;
        }
        
      // If no outlinks, skip the rest.
      if ( uniqueOutlinks.size() == 0 ) return ;

      Text inlink = new Text( );
      for ( String outlink : uniqueOutlinks )
        {          
          // FIXME: Use a Heritrix UURI to do minimal canonicalization
          //        of the toUrl.  This way, it will match the URL if
          //        we actually crawled it.
          String toUrl  = outlink;
          String toHost = getHost( toUrl );

          // If the toHost is null, then there was a serious problem
          // with the URL, so we just skip it.
          if ( toHost == null ) continue;

          // But if the toHost is empty, then assume the URL is a
          // relative URL within the fromHost's site.
          if ( toHost.length() == 0 ) toHost = fromHost;

          // If we are ignoring intra-site links, then skip it.
          if ( ignoreInternalLinks && fromHost.equals( toHost ) ) continue ;
          
          inlink.set( toUrl );

          output.collect( inlink, ONE );
        }
    }

    /**
     * Utility to return the host/domain of a URL, null if URL is
     * malformed.
     */
    public String getHost( String url )
    {
      try
        {
          return idnHelper.getDomain( new URL(url) );
        }
      catch ( Exception e )
        {
          return null;
        }
    }

    /**
     * Utility to return a set of the unique outlinks in a Nutch(WAX) ParseData object.
     */
    public Set<String> getOutlinks( ParseData parsedata )
    {
      Outlink[] outlinks = parsedata.getOutlinks();
      
      if ( outlinks.length == 0 ) return Collections.emptySet();

      Set<String> uniqueOutlinks = new HashSet<String>( outlinks.length );

      for ( Outlink outlink : outlinks )
        {
          uniqueOutlinks.add( outlink.getToUrl().trim() );
        }
      
      return uniqueOutlinks;
    }

    /**
     * Utility to return a set of the unique outlinks in a JBs Document
     */
    public Set<String> getOutlinks( Document document )
    {
      Set<String> uniqueOutlinks = new HashSet<String>( 16 );
      
      for ( Document.Link link : document.getLinks( ) )
        {
          uniqueOutlinks.add( link.getUrl( ) );
        }

      return uniqueOutlinks;
    }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, GenericObject, Text, Text> 
  {
    // For efficiency, only create one instance of the outputKey and outputValue
    private Text outputKey   = new Text();
    private Text outputValue = new Text();

    /**
     * 
     */
    public void reduce( Text key, Iterator<GenericObject> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      long sum = 0;

      Set<String> digests = new HashSet<String>( );

      while ( values.hasNext( ) )
        {
          Writable value = values.next( ).get( );

          if ( value instanceof Text )
            {
              String newDigest = ((Text) value).toString();
              
              digests.add( newDigest );
            }
          else if ( value instanceof LongWritable )
            {
              LongWritable count = (LongWritable) value;
              
              sum += count.get( );
            }
          else
            {
              // Hrmm...should only be one of the previous two.
              LOG.warn( "reduce: unknown value type: " + value );
              continue ;
            }
        }
      
      // Ok, now we have:
      //  key     : the url
      //  digests : all the digests for that url
      //  sum     : num of inlinks to that url
      //
      // Emit all "url digest" combos, with the sum total of the
      // inlinks.  If there is no digest, then we don't emit a record.
      // This is good because if there is no digest, then we do not
      // have any captures for that URL -- i.e.  we never crawled it.

      // If no inlinks, do not bother emitting an output value.
      if ( sum == 0 ) return ;

      // Hand code a trivial JSON string to hold the property.
      outputValue.set( "{\"numInlinks\":\"" + Long.toString(sum) + "\"}" );

      for ( String digest : digests )
        {
          outputKey.set( key + " " + digest ); 
          output.collect( outputKey, outputValue );
        }
    }
  }

  public static class GenericObject extends GenericWritable
  {
    private static Class[] CLASSES = {
      Text        .class, 
      LongWritable.class,
    };

    public GenericObject( )
    {
      super();
    }

    public GenericObject( Writable w )
    {
      super();
      super.set( w );
    }
    
    protected Class[] getTypes()
    {
      return CLASSES;
    }
 }

  public static IDNHelper buildIDNHelper( JobConf job )
    throws IOException
  {
    IDNHelper helper = new IDNHelper( );

    if ( job.getBoolean( "jbs.idnHelper.useDefaults", true ) )
      {
        InputStream is = PageRank.class.getClassLoader( ).getResourceAsStream( "effective_tld_names.dat" );
        
        if ( is == null )
          {
            throw new RuntimeException( "Cannot load default tld rules: effective_tld_names.dat" );
          }
        
        Reader reader = new InputStreamReader( is, "utf-8" );
       
        helper.addRules( reader );
      }

    String moreRules = job.get( "jbs.idnHelper.moreRules", "" );
    
    if ( moreRules.length() > 0 )
      {
        helper.addRules( new StringReader( moreRules ) );
      }

    return helper;
  }

  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(PageRank.class), new PageRank(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if (args.length < 2)
      {
        System.err.println( "PageRank <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), PageRank.class);
    conf.setJobName("jbs.PageRank");
    
    // No need to set this since we use the MultipleInputs class
    // below, which allows us to specify a mapper for each input.
    // conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(GenericObject.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    // The input paths should be either NutchWAX segment directories
    // or Hadoop SequenceFiles containing JSON-encoded Documents
    for ( int i = 1; i < args.length ; i++ )
      {
        Path p = new Path( args[i] );

        // Expand any file globs and then check each matching path
        FileStatus[] files = FileSystem.get( conf ).globStatus( p );

        for ( FileStatus file : files )
          {
            if ( file.isDir( ) )
              {
                // If it's a directory, then check if it is a Nutch segment, otherwise treat as a SequenceFile.
                Path nwp = new Path( file.getPath( ), "parse_data" );
                if ( p.getFileSystem( conf ).exists( nwp ) )
                  {
                    LOG.info( "Adding input path: " + nwp );
                    MultipleInputs.addInputPath( conf, nwp, SequenceFileInputFormat.class, Map.class );
                  }
                else
                  {
                    LOG.info( "Adding input path: " + file.getPath() );
                    MultipleInputs.addInputPath( conf, file.getPath(), SequenceFileInputFormat.class, Map.class );
                  }
              }
            else 
              {
                // Not a directory, skip it.
                LOG.warn( "Not a directory, skip input: " + file.getPath( ) );
              }
          }
      }

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));

    RunningJob rj = JobClient.runJob( conf );
    
    return rj.isSuccessful( ) ? 0 : 1;
  }

}
