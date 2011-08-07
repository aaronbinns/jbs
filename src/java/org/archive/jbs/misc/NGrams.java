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
 *   MapReduce code to generate and count n-grams from the title and
 *   content of records in a NutchWAX segment.  It generates
 *   {2,3,4,5}-grams.
 * </p>
 * <p>
 *   The output is a text file of the form:
 *   <code>ngram, count</code>
 * </p>
 * <p>
 * All the different ngram lengths are mixed together.
 * </p>
 * <p>
 *   <strong>NOTE:</strong> This class is currently experimental and
 *   is not used in production.  If it's ultimately not useful, it will
 *   likely be removed.
 * </p>
 */
public class NGrams extends Configured implements Tool
{

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, LongWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text, LongWritable> output, Reporter reporter)
      throws IOException
    {
      String text = "";

      if ( value instanceof ParseData )
        {
          ParseData pd = (ParseData) value;

          text = pd.getTitle( );
        }
      else if ( value instanceof ParseText )
        {
          text = value.toString( );
        }
      else
        {
          // Weird
          System.out.println( "value type: " + value.getClass( ) );
          return ;
        }

      int lengths[] = { 2, 3, 4, 5 };
      Text tgram = new Text( );
      LongWritable one = new LongWritable( 1 );

      // Strip out anything that is not a letter.
      text = text.replaceAll( "[^\\p{L}]", " " );

      String[] tokens = text.split( "\\s+" );

      for ( int n : lengths )
        {
          for ( int i = 0; i <= ( tokens.length - n ) ; i++ )
            {
              String gram = tokens[i];
              for ( int j = 1 ; j < n ; j++ )
                {
                  gram += " " + tokens[i+j];
                }
              
              tgram.set( gram );
              
              output.collect( tgram, one );
            }
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
  
  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(NGrams.class), new NGrams(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if (args.length < 2)
      {
        System.err.println( "NGrams <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), NGrams.class);
    conf.setJobName("NGrams");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    // FIXME: Do we need this when using the MultipleInputs class below?
    //        Looks like the answer is no.
    // conf.setInputFormat(SequenceFileInputFormat.class);

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
