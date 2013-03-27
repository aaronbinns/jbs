/**
 * Copyright 2013 Internet Archive
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

package org.archive.jbs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.archive.jbs.util.FilenameInputFormat;
import org.archive.jbs.util.PerMapOutputFormat;


/**
 *
 */
public class CDXIndex extends Configured implements Tool 
{
  public static final Log LOG = LogFactory.getLog( CDXIndex.class );

  Configuration conf = null;
  
  public Configuration getConf()
  {
    return conf;
  }

  public void setConf( Configuration conf )
  {
    this.conf = conf;
  }
  
  public int run( String[] args ) throws Exception 
  {
    if ( args.length < 2 )
      {
        return 1;
      }

    FileSystem fs = FileSystem.get( getConf() );

    JobConf job = new JobConf( getConf() );

    job.setJarByClass( CDXIndex.class );
    job.setJobName( "CDXIndex: " + args[0] );

    // Configure the per-map output format to uset he TextOutputFormat
    // class.  Chop off the "(w)arc.gz" from the end of of the input
    // file, but only add "cdx" to the output filename since the
    // TextOutputFormat class will automatically add ".gz" if the
    // output is compressed with gzip.
    job.set( "permap.output.format.class", "org.apache.hadoop.mapred.TextOutputFormat" );
    job.set( "permap.regex",   "w?arc[.]gz$" );
    job.set( "permap.replace", "cdx"       );
    job.setBoolean( "mapred.output.compress", true );
    job.set( "mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec" );

    job.setInputFormat ( FilenameInputFormat.class );
    job.setOutputFormat( PerMapOutputFormat.class );

    job.setOutputKeyClass  (Text.class);
    job.setOutputValueClass(Text.class);

    // This is a map-only job
    job.setNumReduceTasks( 0 );
    job.setMapperClass( CDXIndexMapper.class);
    
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
            Path outputPath = new Path( outputDir, inputPath.getName().replaceAll( "w?arc[.]gz$", "cdx.gz" ) );
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
        LOG.info( "No input files to cdx-index." );
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

  public static void main( String[] args ) throws Exception
  {
    int result = ToolRunner.run( new Configuration(), new CDXIndex(), args );

    System.exit( result );
  }
  
}
