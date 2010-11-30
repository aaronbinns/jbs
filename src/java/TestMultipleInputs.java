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

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;


/**
 * Test/experiment with Hadoop MultipleInputs class, specifically to
 * use different Mappers for different input paths.  It may be
 * possible to use this to implement the "control file" idea.
 */
public class TestMultipleInputs
{

  public static class MultiWritable extends GenericWritable
  {
    private static Class[] CLASSES = {
      NullWritable.class,
      Text.class,
    };
    
    protected Class[] getTypes() {
      return CLASSES;
    }
    
  }


  public static class Mapper1 implements Mapper<Text,Writable,Text,MultiWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text,MultiWritable> output, Reporter reporter )
      throws IOException
    {
      MultiWritable wrapper = new MultiWritable( );
      //wrapper.set( new Text( "mapper1" ) );
      wrapper.set( NullWritable.get() );
      output.collect( key, wrapper );
    }

    public void configure( JobConf job )
    {

    }

    public void close( )
      throws IOException
    {

    }
  }

  public static class Mapper2 implements Mapper<Text,Writable,Text,MultiWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text,MultiWritable> output, Reporter reporter )
      throws IOException
    {
      MultiWritable wrapper = new MultiWritable( );
      wrapper.set( new Text( "mapper2" ) );
      output.collect( key, wrapper );
    }

    public void configure( JobConf job )
    {

    }

    public void close( )
      throws IOException
    {

    }
  }

  public static class Reducer1 implements Reducer<Text,MultiWritable,Text,MultiWritable>
  {
    public void reduce( Text key, Iterator<MultiWritable> values, OutputCollector<Text,MultiWritable> output, Reporter reporter )
      throws IOException
    {
      int c = 0;
      boolean controlSeen = false;

      while ( values.hasNext() )
        {
          MultiWritable wrapper = values.next();

          Writable value = wrapper.get();

          c++;

          if ( value instanceof NullWritable )
            {
              controlSeen = true;
            }

          System.out.println( "" + key + " " + value );
        }

      if ( controlSeen && c > 1 )
        {
          System.out.println( "Skipping: " + key );
        }
      else
        {
          // Bogus value.
          MultiWritable wrapper = new MultiWritable( );
          wrapper.set( new Text( "bogus" ) );
          output.collect( key, wrapper );
        }
    }

    public void configure( JobConf job )
    {

    }

    public void close( )
      throws IOException
    {

    }
  }

  public static void main( String[] args )
    throws Exception
  {
    if (args.length < 3 )
      {
        System.err.println( "TestMultipleInputs <output> <input1> <input2> ..." );
        System.exit(1);
      }
      
    JobConf conf = new JobConf(TestLuceneOutput.class);
    conf.setJobName("TestMultipleInputs");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(MultiWritable.class);
    
    // conf.setMapperClass(Map.class);
    // conf.setCombinerClass(Reducer1.class);
    conf.setReducerClass(Reducer1.class);
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(MapFileOutputFormat.class);

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));

    MultipleInputs.addInputPath( conf, new Path(args[1]), SequenceFileInputFormat.class, Mapper1.class );
    MultipleInputs.addInputPath( conf, new Path(args[2]), SequenceFileInputFormat.class, Mapper2.class );
    
    JobClient.runJob(conf);

  }

}