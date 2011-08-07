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

import java.io.IOException;
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
import org.apache.nutch.metadata.Metadata;

/**
 * <p>
 *   MapReduce code to count URL path parts for records in a NutchWAX
 *   segment.  For example, the URL
 *   <code>http://example.org/some/cool/path.html</code>
 *   would have the path parts:
 *   <code>/some/cool/path.html
 *   /some/cool
 *   /some</code>
 * </p>
 * <p>
 *   The output is a text file of the form:
 *   <code>path-part, count</code>
 * </p>
 * <p>
 *   <strong>NOTE:</strong> This class is currently experimental and
 *   is not used in production.  If it's ultimately not useful, it will
 *   likely be removed.
 * </p>
 */
public class URLPathPartCounter extends Configured implements Tool
{

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, LongWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException
    {
      String urltext = "";

      if ( value instanceof ParseData )
        {
          ParseData pd = (ParseData) value;

          Metadata meta = pd.getContentMeta( );

          urltext = meta.get( "url" );
        }
      else
        {
          // Weird
          System.out.println( "value type: " + value.getClass( ) );
          return ;
        }

      if ( urltext == null || urltext.length() == 0 ) return ;
      
      try
        {
          Text outpath = new Text();
          LongWritable ONE = new LongWritable( 1 );

          URI uri = new URI( urltext );
          
          String path = uri.getRawPath();

          // Skip empty paths.
          if ( path.length() == 0 || path.equals( "/" ) )
            {
              return ;
            }
          
          // Chop off trailing '/'
          if ( path.endsWith( "/" ) )
            {
              path = path.substring( 0, path.length( ) - 1 );
            }

          // Collect the full path.
          outpath.set( path );
          output.collect( outpath, ONE );

          int endpos = path.length(); 
          int pos;
          while ( ( pos = path.lastIndexOf( '/', endpos ) ) > 1 )
            {
              outpath.set( path.substring( 0, pos ) );

              output.collect( outpath, ONE );

              endpos = pos - 1;
            }          
                 
        }
      catch ( URISyntaxException e ) {  }
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
  
  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(URLPathPartCounter.class), new URLPathPartCounter(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if (args.length < 2)
      {
        System.err.println( "URLPathPartCounter <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), URLPathPartCounter.class);
    conf.setJobName("URLPathPartCounter");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    // FIXME: Do we need this when using the MultipleInputs class below?
    //        Looks like the answer is no.
    // conf.setInputFormat(SequenceFileInputFormat.class);

    conf.setOutputFormat(TextOutputFormat.class);
    
    // 
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

            // We don't need the parse_text, just the metadata in parse_data.
            // MultipleInputs.addInputPath( conf, new Path( p, "parse_text" ), SequenceFileInputFormat.class, Map.class );
          }
      }

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));
    
    JobClient.runJob(conf);
    
    return 0;
  }

}
