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

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.*;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.metadata.Metadata;

/**
 * Hadoop MR program which reads from a Nutch(WAX) segment and
 * produces a Lucene index.
 *
 * Although the primary purpose is to test the LuceneOutputFormat
 * class, to actually test that class we need a fairly complete
 * indexing capability.  So, this test class can do end-to-end from
 * the Nutch(WAX) segment to a Lucene index.
 */
public class TestLuceneOutput 
{

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, MapWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException
    {
      MapWritable m = new MapWritable( );

      if ( value instanceof ParseData )
        {
          ParseData pd = (ParseData) value;

          put( m, "title", pd.getTitle( ) );

          Metadata meta = pd.getContentMeta( );
          for ( String name : meta.names( ) )
            {
              put( m, name, meta.get( name ) );
            }
        }
      else if ( value instanceof ParseText )
        {
          put( m, "content_parsed", value.toString() );
        }
      else
        {
          // Weird
          return ;
        }

      output.collect( key, m );
    }

    private void put( MapWritable m, String key, String value )
    {
      if ( value == null ) value = "";

      m.put( new Text( key ), new Text( value ) );
    }

  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, MapWritable, Text, MapWritable> 
  {
    private MultipleOutputs mos;

    public void configure( JobConf conf )
    {
      this.mos = new MultipleOutputs(conf);
    }

    public void reduce( Text key, Iterator<MapWritable> values, OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException
    {
      MapWritable m = new MapWritable( );
      while ( values.hasNext( ) )
        {
          m.putAll( values.next( ) );
        }

      output.collect( key, m );
    }

    public void close( )
      throws IOException 
    {
      mos.close();
    }

  }
  
  public static void main(String[] args) throws Exception
  {
    if (args.length != 2)
      {
        System.err.println( "TestLuceneOutput <input> <output>" );
        System.exit(1);
      }
      
    JobConf conf = new JobConf(TestLuceneOutput.class);
    conf.setJobName("wordcount");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(MapWritable.class);
    
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setInputFormat(SequenceFileInputFormat.class);

    conf.setOutputFormat(LuceneOutputFormat.class);

    // Assume arg[0] is a Nutch(WAX) segment
    Path base = new Path( args[0] );
    FileInputFormat.addInputPath(conf, new Path( base, "parse_data"));
    FileInputFormat.addInputPath(conf, new Path( base, "parse_text"));

    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient.runJob(conf);
  }

}
