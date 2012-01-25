/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.archive.jbs;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.arc.ARCReader;

import org.archive.jbs.arc.ArcReader;

/**
 * Parse the contents of a (W)ARC file, output
 * in a JSON Document.
 */
public class Import extends Configured implements Tool
{

  public static final Log LOG = LogFactory.getLog( Import.class );

  public static class ImportMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
  {
    private JobConf        jobConf;
    private ParseUtil      parseUtil;
    private HTTPStatusCodeFilter httpStatusCodeFilter;
      
    /**
     * <p>Configures the job.  Sets the url filters, scoring filters, url normalizers
     * and other relevant data.</p>
     * 
     * @param job The job configuration.
     */
    public void configure( JobConf job )
    {
      this.jobConf = job;
      this.parseUtil = new ParseUtil( jobConf );
      this.httpStatusCodeFilter = new HTTPStatusCodeFilter( jobConf.get( "nutchwax.filter.http.status" ) );
    }
    
    /**
     * <p>Runs the Map job to import records from an archive file into a
     * Nutch segment.</p>
     * 
     * @param key Line number in manifest corresponding to the <code>value</code>
     * @param value A line from the manifest
     * @param output The output collecter.
     * @param reporter The progress reporter.
     */
    public void map( Text key, Text value, OutputCollector output, Reporter reporter )
      throws IOException
    {
      String path = key.toString();
      
      FSDataInputStream fis = null;
      try
        {
          fis = FileSystem.get(this.jobConf).open( new Path( path ) );

          ArcReader reader = new ArcReader( path, fis );
          
          for ( ARCRecord record : reader )
            {
              // When reading WARC files, records of type other than
              // "response" are returned as 'null' by the Iterator, so
              // we skip them.
              if ( record == null ) continue ;
              
              importRecord( record, output );
              
              reporter.progress();
            }
        }
      catch ( Exception e )
        {
          LOG.error( "Error processing archive file: " + path, e );
          
          if ( jobConf.getBoolean( "nutchwax.import.abortOnArchiveReadError", false ) )
            {
              throw new IOException( e );
            }
        }
      finally
        {
          if ( LOG.isInfoEnabled() ) 
            {
              LOG.info( "Completed ARC: "  + path );
            }
        }
    }
    
    /**
     * Import an ARCRecord.
     *
     * @param record
     * @param segmentName 
     * @param output
     * @return whether record was imported or not (i.e. filtered out due to URL filtering rules, etc.)
     */
    private boolean importRecord( ARCRecord record, OutputCollector output )
    {
      ARCRecordMetaData meta = record.getMetaData();
      
      if ( LOG.isInfoEnabled() ) LOG.info( "Consider URL: " + meta.getUrl() + " (" + meta.getMimetype() + ") [" + meta.getLength( ) + "]" );
      
      if ( ! this.httpStatusCodeFilter.isAllowed( record.getStatusCode( ) ) )
        {
          if ( LOG.isInfoEnabled() ) LOG.info( "Skip     URL: " + meta.getUrl() + " HTTP status:" + record.getStatusCode() );
          
          return false;
        }
      
      try
        {
          // Skip the HTTP headers in the response body, so that the
          // parsers are parsing the reponse body and not the HTTP
          // headers.
          record.skipHttpHeader();
          
          // We use record.available() rather than meta.getLength()
          // because the latter includes the size of the HTTP header,
          // which we just skipped.
          long length = record.available();
          byte[] bytes = readBytes( record, length );
          
          // If there is no digest, then we assume we're reading an
          // ARCRecord not a WARCRecord.  In that case, we close the
          // record, which updates the digest string.  Then we tweak the
          // digest string so we have the same for for both ARC and WARC
          // records.
          if ( meta.getDigest() == null )
            {
              record.close();
              
              // This is a bit hacky, but ARC and WARC records produce
              // two slightly different digest formats.  WARC record
              // digests have the algorithm name as a prefix, such as
              // "sha1:PD3SS4WWZVFWTDC63RU2MWX7BVC2Y2VA" but the
              // ArcRecord.getDigestStr() does not.  Since we want the
              // formats to match, we prepend the "sha1:" prefix to ARC
              // record digest.
              meta.setDigest( "sha1:" + record.getDigestStr() );
            }

          String url = meta.getUrl();
          String key = url + " " + meta.getDigest( );
          
          Metadata contentMetadata = new Metadata();

          // We store both the normal URL and the URL+digest key for
          // later retrieval by the indexing plugin(s).
          contentMetadata.set( "url",     url  );
          contentMetadata.set( "date",   meta.getDate()     );
          contentMetadata.set( "digest", meta.getDigest()   );
          contentMetadata.set( "length", String.valueOf( meta.getLength() ) );
          contentMetadata.set( "code",   String.valueOf( record.getStatusCode() ) );
          
          String type = (meta.getMimetype( ) == null ? "" : meta.getMimetype( )).split( "[;]" )[0].toLowerCase().trim();
          
          // If the Content-Type from the HTTP response is "text/plain",
          // set it to null to trigger full auto-detection via Tika.
          if ( "text/plain".equals( type ) )
            {
              type = null;
            }
          
          Content content = new Content( url, url, bytes, type, contentMetadata, this.jobConf );
          
          if ( LOG.isInfoEnabled() ) LOG.info( "Auto-detect content-type: " + type + " " + content.getContentType( ) + " " + url );
          
          // Store both the original and auto-detected content types.
          contentMetadata.set( "type",  content.getContentType( ) );
          
          if ( "text/html"            .equals( content.getContentType( ) ) || 
               "application/xhtml+xml".equals( content.getContentType( ) ) ||
               "application/xhtml"    .equals( content.getContentType( ) ) )
            {
              int size = jobConf.getInt( "nutchwax.import.content.limit.html", -1 );
              if ( size > 0 && size < length )
                {
                  LOG.warn( "HTML file size exceeds threshold [" + size + "]: " + meta.getUrl( ) + " [" + length + "]" );
                  
                  bytes = Arrays.copyOf( bytes, size );
                  
                  content.setContent( bytes );
                }
              
              try
                {
                  if ( jobConf.getBoolean( "nutchwax.import.boilerpipe", false ) )
                    {
                      // BoilerPipe!
                      contentMetadata.set( "boiled", de.l3s.boilerpipe.extractors.DefaultExtractor.INSTANCE.getText( new org.xml.sax.InputSource( new java.io.ByteArrayInputStream( bytes ) ) ) );
                    }
                }
              catch ( Exception e ) 
                { 
                  LOG.warn( "Error boilerpiping: " + meta.getUrl( ) ); 
                }
            }
          
          if ( "text/plain".equals( content.getContentType( ) ) )
            {
              int size = jobConf.getInt( "nutchwax.import.content.limit.text", -1 );
              if ( size > 0 && size < length )
                {
                  LOG.warn( "Text file size exceeds threshold [" + size + "]: " + meta.getUrl( ) + " [" + length + "]" );
                  
                  bytes = Arrays.copyOf( bytes, size );
                  
                  content.setContent( bytes );
                }
            }
          
          output( output, new Text( key ), content );
          
          return true;
        }
      catch ( Throwable t )
        {
          LOG.error( "Import fail : " + meta.getUrl(), t );
        }
      
      return false;
    }
        
    /**
     * Writes the key and related content to the output collector.  The
     * division between <code>importRecord</code> and
     * <code>output</code> is merely based on the way the code was
     * structured in the <code>ArcSegmentCreator.java</code> which was
     * used as a starting-point for this class.
     */
    private void output( OutputCollector output,
                         Text            key,
                         Content         content )
    {
      LOG.info( "output( " + key + " )" );
      
      ParseResult parseResult = null;
      try
        {
          parseResult = this.parseUtil.parse( content );
        }
      catch ( Throwable t )
        {
          if ( LOG.isInfoEnabled() ) LOG.info( "Error parsing: " + key, t );
        }
      
      try
        {
          if ( parseResult != null )
            {
              for ( Map.Entry<Text, Parse> entry : parseResult )
                {
                  Text  url   = entry.getKey();
                  Parse parse = entry.getValue();
                  ParseStatus parseStatus = parse.getData().getStatus();
                  
                  if ( !parseStatus.isSuccess() )
                    {
                      LOG.warn( "Error parsing: " + key + ": " + parseStatus );
                      parse = parseStatus.getEmptyParse( this.jobConf );
                    }
                  
                  String parsedText = parse.getText();
                  
                  // TODO: Limit size of parsedText.

                  Document doc = new Document();

                  ParseData pd = parse.getData();

                  doc.set( "title", pd.getTitle( ) );
                  
                  Metadata meta = pd.getContentMeta( );
                  for ( String name : new String[] { "url", "digest", "length", "collection", "boiled", "date", "type", "keywords", "description" } )
                    {
                      doc.set( name, meta.get( name ) );
                    }
                  
                  for ( Outlink outlink : pd.getOutlinks( ) )
                    {
                      doc.addLink( outlink.getToUrl( ), outlink.getAnchor( ) );
                    }
                  
                  doc.set( "content_parsed", parsedText );

                  // Emit JSON string
                  output.collect( key, new Text( doc.toString() ) );
                }
            }
        }
      catch ( Exception e )
        {
          LOG.error( "Error outputting Nutch record for: " + key, e );
        }
    }
    
    /**
     * Utility method to read the content bytes from an archive record.
     * The number of bytes read can be limited via the configuration
     * property <code>nutchwax.import.content.limit</code>.
     */
    private byte[] readBytes( ArchiveRecord record, long contentLength )
      throws IOException
    {
      // Ensure the record does strict reading.
      record.setStrict( true );
      
      long size = jobConf.getLong( "nutchwax.import.content.limit", -1 );
      
      if ( size < 0 )
        {
          size = contentLength;
        }
      else
        {
          size = Math.min( size, contentLength );
        }
      
      // Read the bytes of the HTTP response
      byte[] bytes = new byte[(int) size];
      
      if ( size == 0 )
        {
          return bytes;
        }
      
      // NOTE: Do not use read(byte[]) because ArchiveRecord does NOT over-ride
      //       the implementation inherited from InputStream.  And since it does
      //       not over-ride it, it won't do the digesting on it.  Must use either
      //       read(byte[],offset,length) or read().
      int pos = 0;
      while ( (pos += record.read( bytes, pos, (bytes.length - pos) )) < bytes.length )
        ;
      
      // Now that the bytes[] buffer has been filled, read the remainder
      // of the record so that the digest is computed over the entire
      // content.
      byte[] buf = new byte[1024 * 1024];
      int count = 0;
      while ( record.available( ) > 0 )
        {
          count += record.read( buf, 0, Math.min( buf.length, record.available( ) ) );
        }
      
      if ( LOG.isInfoEnabled() ) LOG.info( "Bytes read: expected=" + contentLength + " bytes.length=" + bytes.length + " pos=" + pos + " count=" + count );
      
      // Sanity check.  The number of bytes read into our bytes[]
      // buffer, plus the count of extra stuff read after it should
      // equal the contentLength passed into this function.
      if ( pos + count != contentLength )
        {
          throw new IOException( "Incorrect number of bytes read from ArchiveRecord: expected=" + contentLength + " bytes.length=" + bytes.length + " pos=" + pos + " count=" + count );
        }
      
      return bytes;
    }

  }

  /**
   * Run the job.
   */
  public int run( String[] args ) throws Exception
  {
    if ( args.length < 2 )
      {
        usage();
        return 1;
      }

    try
      {
        FileSystem fs = FileSystem.get( getConf() );

        // Create a job configuration
        JobConf job = new JobConf( getConf( ) );

        // FIXME: Can we give a better name?
        job.setJobName( "jbs.Import" );
        
        // Required to configure the Nutch(WAX) plugins and stuff.
        job.addResource("nutch-default.xml");
        job.addResource("nutch-site.xml");

        // The inputs are a list of filenames, use the
        // FilenameInputFormat to pass them to the mappers.
        job.setInputFormat( FilenameInputFormat.class );

        // This is a map-only job, no reducers.
        job.setNumReduceTasks(0);
        
        // Configure the MultipleSequenceFileOutputFormat so that the
        // output of the map tasks are named according to their input
        // files.  Thus, and input file of "foo.warc.gz" results in a
        // map output file of "foo.warc.gz".
        job.setOutputFormat( MultipleSequenceFileOutputFormat.class );
        job.setInt( "mapred.outputformat.numOfTrailingLegs", 1 );

        // Use our ImportMapper, with output keys and values of type
        // Text.
        job.setMapperClass( ImportMapper.class );
        job.setOutputKeyClass  ( Text.class );
        job.setOutputValueClass( Text.class );

        // Configure the input and output paths, from the command-line.
        for ( int i = 0 ; i < (args.length-1) ; i++ )
          {
            FileInputFormat.addInputPath( job, new Path( args[i] ) );
          }
        FileOutputFormat.setOutputPath( job, new Path( args[args.length-1] ) );
        
        // Run the job!
        RunningJob rj = JobClient.runJob( job );
        
        if ( ! rj.isSuccessful( ) )
          {
            LOG.error( "FAILED: " + rj.getID() );
            return 2;
          }

        return 0;
      }
    catch ( Exception e )
      {
        LOG.fatal( "FAILED: ", e );
        return 2;
      }
  }

  /**
   * Emit usage information for command-line driver.
   */
  public void usage( )
  {
    String usage = 
        "Usage: Import <(w)arcfiles...> <outputDir>\n" 
      ;
    
    System.out.println( usage );
  }

  /**
   * Command-line driver.  Runs the Import as a Hadoop job.
   */
  public static void main( String args[] ) throws Exception
  {
    int result = ToolRunner.run( new JobConf(Import.class), new Import(), args );

    System.exit( result );
  }

}
