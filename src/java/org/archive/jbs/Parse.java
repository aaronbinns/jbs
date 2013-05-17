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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
//import org.apache.nutch.parse.Parse;  // Don't import due to name conflict.
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;

import org.archive.io.warc.WARCConstants;

import org.archive.jbs.arc.ArcReader;
import org.archive.jbs.arc.ArchiveRecordProxy;

import org.archive.jbs.util.FilenameInputFormat;
import org.archive.jbs.util.PerMapOutputFormat;

/**
 * Parse the contents of a (W)ARC file, output
 * in a JSON Document.
 */
public class Parse extends Configured implements Tool
{

  public static final Log LOG = LogFactory.getLog( Parse.class );

  public static class ParseMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> 
  {
    private JobConf        jobConf;
    private ParseUtil      parseUtil;
      
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
    }
    
    /**
     * Read the records from the (w)arc file named in the
     * <code>key</code>, parse each record (if possible) and emit a
     * JSON Document for the parsed record body.
     */
    public void map( Text key, Text value, OutputCollector output, Reporter reporter )
      throws IOException
    {
      String path = key.toString();

      LOG.info( "Start: "  + path );
      
      FSDataInputStream fis = null;
      try
        {
          fis = FileSystem.get( new java.net.URI( path ), this.jobConf ).open( new Path( path ) );

          ArcReader reader = new ArcReader( path, fis );

          reader.setSizeLimit( jobConf.getInt( "jbs.parse.content.limit", -1 ) );
          
          for ( ArchiveRecordProxy record : reader )
            {
              // If this is an HTTP response record, do all the parsing and stuff.
              if ( WARCConstants.WARCRecordType.RESPONSE.toString().equals( record.getWARCRecordType() ) )
                {
                  if ( WARCConstants.HTTP_RESPONSE_MIMETYPE.equals( record.getWARCContentType() ) )
                    {
                      LOG.info( "Process response: " + record.getUrl() + " digest:" + record.getDigest() + " date: " + record.getDate() );
                      
                      parseRecord( record, output );
                    }
                  else
                    {
                      LOG.info( "Skip response: " + record.getUrl() + " response-type:" + record.getWARCContentType() + " date: " + record.getDate() );
                    }
                }
              else if ( WARCConstants.WARCRecordType.RESOURCE.toString().equals( record.getWARCRecordType() ) )
                {
                  // We only care about "ftp://" resource records.  It's possible that the ArchiveRecordProxy will
                  // pass us resource records other than ftp, so we filter out non-ftp ones here.
                  //
                  // Also, only care about "application/octet-stream" content-type, which indicates the record is
                  // the ftp file download and a directory listing.
                  if ( record.getUrl().startsWith( "ftp://" ) 
                       &&
                       "application/octet-stream".equals( record.getWARCContentType() )
                     )
                    {
                      LOG.info( "Process resource: " + record.getUrl() + " digest:" + record.getDigest() + " date: " + record.getDate() );

                      parseRecord( record, output );
                    }
                  else
                    {
                      LOG.info( "Skip resource: " + record.getUrl() + " response-type:" + record.getWARCContentType() + " date: " + record.getDate() );
                    }
                }
              else if ( WARCConstants.WARCRecordType.REVISIT.toString().equals( record.getWARCRecordType() ) )
                {
                  // If this is a revisit record, just create a JSON
                  // Document with the relevant info.  No parsing or
                  // anything needed.
                  LOG.info( "Process revisit: " + record.getUrl() + " digest:" + record.getDigest() + " date: " + record.getDate() );

                  Text docKey = new Text( record.getUrl() + " " + record.getDigest( ) );

                  Document doc = new Document();
                  doc.set( "url",    record.getUrl() );
                  doc.set( "digest", record.getDigest() );
                  doc.set( "date",   record.getDate() );
                  
                  output.collect( docKey, new Text( doc.toString() ) );
                 }
              else 
                {
                  LOG.info( "Skip record: " + record.getUrl() + " record-type:" + record.getWARCRecordType() + " date: " + record.getDate() );
                }
              
              reporter.progress();
            }
        }
      catch ( Exception e )
        {
          LOG.error( "Error processing archive file: " + path, e );
          
          if ( jobConf.getBoolean( "jbs.parse.abortOnArchiveReadError", true ) )
            {
              throw new IOException( e );
            }
        }
      finally
        {
          LOG.info( "Finish: "  + path );
        }
    }
    
    /**
     * 
     */
    private void parseRecord( ArchiveRecordProxy record, OutputCollector output )
      throws IOException
    {
      String key = record.getUrl() + " " + record.getDigest( );

      try
        {
          Metadata contentMetadata = new Metadata();
          contentMetadata.set( "url",    record.getUrl()      );
          contentMetadata.set( "date",   record.getDate()     );
          contentMetadata.set( "digest", record.getDigest()   );
          contentMetadata.set( "length", String.valueOf( record.getLength() ) );
          contentMetadata.set( "code",   record.getHttpStatusCode() );
          
          // The Nutch Content object will invoke Tika's magic/mime-detection.
          Content content = new Content( record.getUrl(), record.getUrl(), record.getHttpResponseBody(), null, contentMetadata, this.jobConf );

          // Retain the auto-detected Content-Type/MIME-Type.
          contentMetadata.set( "type",  content.getContentType( ) );

          // Limit the size of either the HTML or text document to avoid blowing up the parsers.
          // Also boilerpipe the HTML.
          if ( "text/html"            .equals( content.getContentType( ) ) || 
               "application/xhtml+xml".equals( content.getContentType( ) ) ||
               "application/xhtml"    .equals( content.getContentType( ) ) )
            {
              int size = jobConf.getInt( "jbs.parse.content.limit.html", -1 );
              if ( size > 0 && size < record.getLength() )
                {
                  LOG.warn( "HTML file size exceeds threshold [" + size + "]: " + record.getUrl( ) + " [" + record.getLength() + "]" );
                  
                  content.setContent( Arrays.copyOf( record.getHttpResponseBody(), size ) );
                }
              
              try
                {
                  if ( jobConf.getBoolean( "jbs.parse.boilerpipe", true ) )
                    {
                      // BoilerPipe!
                      contentMetadata.set( "boiled", de.l3s.boilerpipe.extractors.DefaultExtractor.INSTANCE.getText( new org.xml.sax.InputSource( new java.io.ByteArrayInputStream( record.getHttpResponseBody() ) ) ) );
                    }
                }
              catch ( Exception e ) 
                { 
                  LOG.warn( "Error boilerpiping: " + record.getUrl( ) ); 
                }
            }
          
          if ( "text/plain".equals( content.getContentType( ) ) )
            {
              int size = jobConf.getInt( "jbs.parse.content.limit.text", -1 );
              if ( size > 0 && size < record.getLength() )
                {
                  LOG.warn( "Text file size exceeds threshold [" + size + "]: " + record.getUrl( ) + " [" + record.getLength() + "]" );
                  
                  content.setContent( Arrays.copyOf( record.getHttpResponseBody(), size ) );
                }
            }
          
          write( output, new Text( key ), content );
        }
      catch ( Throwable t )
        {
          if ( jobConf.getBoolean( "jbs.parse.emitParseErrorRecords", true ) )
            {
              Document doc = new Document();
              doc.set( "status", "error" );
              doc.set( "errorMessage", "Failed to parse record: " + t.getMessage() );
              
              output.collect( new Text( key ), new Text( doc.toString() ) );
            }
        }
    }
        
    /**
     * Writes the key and related content to the output collector.
     */
    private void write( OutputCollector output,
                        Text            key,
                        Content         content )
      throws IOException
    {
      ParseResult parseResult = null;
      try
        {
          parseResult = this.parseUtil.parse( content );
        }
      catch ( Throwable t )
        {
          if ( jobConf.getBoolean( "jbs.parse.emitParseErrorRecords", true ) )
            {
              Document doc = new Document();
              doc.set( "status", "error" );
              doc.set( "errorMessage", "Failed to parse record: " + t.getMessage() );
              
              output.collect( key, new Text( doc.toString() ) );
            }
        }
      
      try
        {
          if ( parseResult != null )
            {
              for ( Map.Entry<Text, org.apache.nutch.parse.Parse> entry : parseResult )
                {
                  // Text url = entry.getKey();
                  org.apache.nutch.parse.Parse parse = entry.getValue();
                  ParseStatus parseStatus = parse.getData().getStatus();
                  
                  if ( !parseStatus.isSuccess() )
                    {
                      LOG.warn( "Error parsing: " + key + ": " + parseStatus );
                      parse = parseStatus.getEmptyParse( this.jobConf );
                    }
                  
                  String parsedText = parse.getText();
                  
                  Document doc = new Document();

                  ParseData pd = parse.getData();

                  // Copy metadata fields.
                  Metadata meta = pd.getContentMeta( );
                  for ( String name : meta.names( ) )
                    {
                      doc.set( name, meta.get( name ) );
                    }
                  
                  // Ensure that the title comes from the ParseData.
                  doc.set( "title", pd.getTitle( ) );
                  
                  // Optionally skip the outlinks.
                  if ( jobConf.getBoolean( "jbs.parse.emitOutlinks", true ) )
                    {
                      for ( Outlink outlink : pd.getOutlinks( ) )
                        {
                          doc.addLink( outlink.getToUrl( ), outlink.getAnchor( ) );
                        }
                    }

                  doc.set( "content", parsedText );
                  
                  // Emit JSON string
                  output.collect( key, new Text( doc.toString() ) );
                }
            }
        }
      catch ( Throwable t )
        {
          if ( jobConf.getBoolean( "jbs.parse.emitParseErrorRecords", true ) )
            {
              Document doc = new Document();
              doc.set( "status", "error" );
              doc.set( "errorMessage", "Failed to parse record: " + t.getMessage() );
              
              output.collect( key, new Text( doc.toString() ) );
            }
        }
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

    FileSystem fs = FileSystem.get( getConf() );
    
    // Create a job configuration
    JobConf job = new JobConf( getConf( ) );
    
    // Job name uses output dir to help identify it to the operator.
    job.setJobName( "jbs.Parse " + args[0] );
    
    // The inputs are a list of filenames, use the
    // FilenameInputFormat to pass them to the mappers.
    job.setInputFormat( FilenameInputFormat.class );
    
    // This is a map-only job, no reducers.
    job.setNumReduceTasks(0);
    
    // Use the Parse-specific output format.
    job.setOutputFormat( PerMapOutputFormat.class );
    
    // Use our ParseMapper, with output keys and values of type
    // Text.
    job.setMapperClass( ParseMapper.class );
    job.setOutputKeyClass  ( Text.class );
    job.setOutputValueClass( Text.class );
    
    // Configure the input and output paths, from the command-line.
    Path outputDir = new Path( args[0] );
    FileOutputFormat.setOutputPath( job, outputDir );
    
    boolean atLeastOneInput = false;
    for ( int i = 1 ; i < args.length ; i++ )
      {
        FileSystem inputfs = FileSystem.get( new java.net.URI( args[i] ), getConf() );

        for ( FileStatus status : inputfs.globStatus( new Path( args[i] ) ) )
          {
            Path inputPath  = status.getPath();
            Path outputPath = new Path( outputDir, inputPath.getName() );
            if ( fs.exists( outputPath ) )
              {
                LOG.debug( "Output path already exists: " + outputPath );
              }
            else
              {
                atLeastOneInput = true;
                LOG.info( "Add input path: " + inputPath );
                FileInputFormat.addInputPath( job, inputPath );
              }
          }
      }
    
    if ( ! atLeastOneInput )
      {
        LOG.info( "No input files to parse." );
        return 0;
      }
    
    // Run the job!
    RunningJob rj = JobClient.runJob( job );
    
    if ( ! rj.isSuccessful( ) )
      {
        LOG.error( "FAILED: " + rj.getID() );
        return 2;
      }
    
    return 0;
  }

  /**
   * Emit usage information for command-line driver.
   */
  public void usage( )
  {
    String usage =  "Usage: Parse <outputDir> <(w)arcfile>...\n" ;
    
    System.out.println( usage );
  }

  /**
   * Command-line driver.  Runs the Parse as a Hadoop job.
   */
  public static void main( String args[] ) throws Exception
  {
    JobConf conf = new JobConf(Parse.class);

    // Load the default set of config properties, including the
    // essential properties needed by the bits of Nutch that we are
    // still using.  These properties can still be over-ridden by
    // command-line args.
    conf.addResource( "conf-parse.xml" );

    int result = ToolRunner.run( conf, new Parse(), args );

    System.exit( result );
  }

}
