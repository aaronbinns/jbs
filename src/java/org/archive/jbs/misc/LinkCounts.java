/*
 * Copyright 2010 Internet Archive
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

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.metadata.Metadata;

/**
 * <p>
 *   MapReduce code to count links between sties, based on the 
 *   links in a NutchWAX segment.
 * </p>
 * <p>
 *   <strong>NOTE:</strong> This class is currently experimental and
 *   is not used in production.  If it's ultimately not useful, it will
 *   likely be removed.
 * </p>
 */
public class LinkCounts extends Configured implements Tool
{
  private static boolean ignoreInternalLinks = false;

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, LongWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException
    {
      if ( ! ( value instanceof ParseData ) )
        {
          // Don't care if this is something other than the ParseData.
          return ;
        }
        
      ParseData pd = (ParseData) value;
     
      String fromHost = getHost( key.toString() );

      // If there is no fromHost, skip it.
      if ( fromHost == null || fromHost.length() == 0 ) return;
 
      Outlink[] outlinks = pd.getOutlinks();
 
      // If no outlinks, skip the rest.
      if ( outlinks.length == 0 ) return ;

      Text link = new Text( );
      LongWritable one = new LongWritable( 1 );

      for (int i = 0; i < outlinks.length; i++) 
        {
          Outlink outlink = outlinks[i];
          String toUrl = outlink.getToUrl();
          
          String toHost = getHost( toUrl );

          // If the toHost is null, then there was a serious problem
          // with the URL, so we just skip it.
          if ( toHost == null ) continue;

          // But if the toHost is empty, then assume the URL is a
          // relative URL within the fromHost's site.
          if ( toHost.length() == 0 ) toHost = fromHost;

          if ( ignoreInternalLinks && fromHost.equals( toHost ) ) continue ;
          
          String prefix = fromHost.equals(toHost) ? "0" : "1";

          link.set( prefix + " " + fromHost.trim() + " " + toHost.trim() );

          output.collect( link, one );
        }
    }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> 
  {
    public void reduce( Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException
    {
      long sum = 0;

      while ( values.hasNext( ) )
        {
          LongWritable value = values.next( );
          
          sum += value.get( );
        }
      
      output.collect( key, new LongWritable( sum ) );
    }
  }

  public static String getHost( String url ) 
  {
    try 
      {
        String host = (new URL(url)).getHost();

        // Strip off any "www[0-9]*." header.
        host = host.toLowerCase().replaceFirst( "^www[0-9]*[.]", "" );

        // Special rule for Photobucket
        host = host.replaceAll( "^[a-z0-9]+.photobucket.com$", "photobucket.com" );

        return host.trim();
      }
    catch ( MalformedURLException e )
      {
        return null;
      }
  }
  
  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(LinkCounts.class), new LinkCounts(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if (args.length < 2)
      {
        System.err.println( "LinkCounts <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), LinkCounts.class);
    conf.setJobName("LinkCounts");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setOutputFormat(TextOutputFormat.class);
    
    // Assume the inputs are NutchWAX segments.
    for ( int i = 1; i < args.length ; i++ )
      {
        Path p = new Path( args[i] );

        if ( p.getFileSystem( conf ).isFile( p ) )
          {
            // FIXME: Emit an error message.
          }
        else
          {       
            MultipleInputs.addInputPath( conf, new Path( p, "parse_data" ), SequenceFileInputFormat.class, Map.class );
            MultipleInputs.addInputPath( conf, new Path( p, "parse_text" ), SequenceFileInputFormat.class, Map.class );
          }
      }

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));
    
    JobClient.runJob(conf);
    
    return 0;
  }

}
