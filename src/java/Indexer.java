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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.metadata.Metadata;

/** 
 * Command-line driver and MapReduce code for creating a Lucene index
 * for one or more NutchWAX segments and text revisit/.dup files.
 */
public class Indexer extends Configured implements Tool
{
  /**
   * Mapper that handles text files with revisit lines of the form:
   *   URL hash date
   */
  public static class RevisitMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>
  {
    public void map( LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter )
      throws IOException
    {
      try
        {
          String[] line = value.toString().trim().split("\\s+");

          Text newKey   = null;
          Text newValue = null;

          switch ( line.length )
            {
              // Handle lines from "dup" files.
            case 3:
              newKey   = new Text( line[0] + " " + line[1] );
              newValue = new Text( line[2] );
              break ;

              // Handle lines from "cdx" files.
            case 9:
              newKey   = new Text( line[0] + " sha1:" + line[5] );
              newValue = new Text( line[1] );
              break ;

            default:
              // Skip it
              return ;
            }

          output.collect( newKey, newValue );     
        }
      catch ( Exception e )
        {
          // Eat it.
        }
    }
  }

  /**
   * Mapper that can handle Writables from Nutch(WAX) segments.
   */
  public static class NutchMapper extends MapReduceBase implements Mapper<Text, Writable, Text, MapWritable>
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
      else if ( value instanceof Text )
        {
          System.out.println( "Dup: " + key.toString() + " " + value.toString( ) );
          put( m, "date", value.toString() );
        }
      else
        {
          // Weird
          System.out.println( "value type: " + value.getClass( ) );
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
    public void reduce( Text key, Iterator<MapWritable> values, OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException
    {
      MapWritable m = new MapWritable( );

      // Take the [k,v] pairs from each MapWritable and merge them
      // intelligently.  In particular the dates.  E.g. we might have
      //   MapWritable1 = [ "date" => "20100501..."
      //   MapWritable2 = [ "date" => "20081219..."
      // We want to have
      //   Merged       = [ "date" => ["20100501...","20081219..."] ]
      // with one key "date" and multiple values.
      while ( values.hasNext( ) )
        {
          MapWritable properties = values.next( );

          for ( Writable writableKey : properties.keySet( ) )
            {
              Text propkey = (Text) writableKey;
              Text propval = (Text) properties.get( writableKey );

              Text currentValue = (Text) m.get( writableKey );

              // If multiple date values, concatenate them, separated by space.
              if ( currentValue != null && "date".equals( propkey.toString() ) )
                {
                  propval = new Text( propval.toString() + " " + currentValue.toString() );
                }
              
              m.put( propkey, propval );
            }
        }
      
      output.collect( key, m );
    }
  }
  
  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(Indexer.class), new Indexer(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if ( args.length < 2 )
      {
        System.err.println( "Indexer <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), Indexer.class);
    conf.setJobName("Indexer");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(MapWritable.class);
    
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    // Choose the outputformat to either merge or index the records
    //   LuceneOutputFormat  - writes to Lucene index.
    //   SolrOutputFormat    - sends documents to external Solr server
    //   MapFileOutputFormat - merges documents into Hadoop MapFile
    //                         org.apache.hadoop.mapred.MapFileOutputFormat
    conf.setOutputFormat( (Class) Class.forName( conf.get( "indexer.outputformat.class", "LuceneOutputFormat" ) ) );

    // For debugging, sometimes easier to inspect Hadoop mapfile format.
    // conf.setOutputFormat(MapFileOutputFormat.class);
    
    // Add the input paths as either NutchWAX segment directories or
    // text .dup files.
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
                if ( p.getFileSystem( conf ).exists( new Path( file.getPath( ), "parse_data" ) ) )
                  {
                    MultipleInputs.addInputPath( conf, new Path( p, "parse_data" ), SequenceFileInputFormat.class, NutchMapper.class );
                    MultipleInputs.addInputPath( conf, new Path( p, "parse_text" ), SequenceFileInputFormat.class, NutchMapper.class );
                  }
                else
                  {
                    MultipleInputs.addInputPath( conf, p, SequenceFileInputFormat.class, IdentityMapper.class );
                  }
              }
            else 
              {
                // Not a directory, assume it's a cdx/dup text file.
                MultipleInputs.addInputPath( conf, new Path( args[i] ), TextInputFormat.class, RevisitMapper.class );
              }
          }
      }

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));
    FileOutputFormat.setCompressOutput( conf, true );
    
    JobClient.runJob(conf);
    
    return 0;
  }

}
