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

package org.archive.jbs;

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
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.metadata.Metadata;

/** 
 * Command-line driver and MapReduce code for processing full-text
 * documents, currently originating from a NutchWAX import action.
 *
 * This class can be used to merge multiple NutchWAX segments, along
 * with CDX info to produce a single merged/deduped collection of
 * Document objects.
 *
 * This class can take that merged/deduped collection of Documents
 * and write them to various destinations:
 *   Hadoop file  -- stores the merge/deduped set in a Hadoop file
 *   Lucene index -- builds a full-text Lucene index of the Docs
 *   Solr index   -- pushes the Documents to a Solr server.
 *
 * NOTE: Previously, this class was called 'Indexer', but once it
 * gained the ability to simply store the merge/deduped set of
 * Documents in a Hadoop file, the name needed changed.
 */
public class Main extends Configured implements Tool
{
  /**
   * Mapper that handles text files of various formats, primarily CDX and "revisit" files.
   */
  public static class TextMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DocumentWritable>
  {
    public void map( LongWritable key, Text value, OutputCollector<Text,DocumentWritable> output, Reporter reporter )
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
          
          DocumentWritable doc = new DocumentWritable( );
          doc.add( "date", newValue.toString( ) );

          output.collect( newKey, doc );
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
  public static class NutchMapper extends MapReduceBase implements Mapper<Text, Writable, Text, DocumentWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text, DocumentWritable> output, Reporter reporter)
      throws IOException
    {
      DocumentWritable doc = new DocumentWritable( );

      if ( value instanceof ParseData )
        {
          ParseData pd = (ParseData) value;

          doc.set( "title", pd.getTitle( ) );
          
          Metadata meta = pd.getContentMeta( );
          for ( String name : new String[] { "url", "digest", "length", "collection", "boiled", "date", "type" } )
            {
              doc.set( name, meta.get( name ) );
            }

          for ( Outlink outlink : pd.getOutlinks( ) )
            {
              doc.addLink( outlink.getToUrl( ), outlink.getAnchor( ) );
            }
        }
      else if ( value instanceof ParseText )
        {
          doc.set( "content_parsed", value.toString() );
        }
      else
        {
          // Weird
          System.out.println( "NutchMapper unknown value type: " + value.getClass( ) );
          return ;
        }

      output.collect( key, doc );
    }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, DocumentWritable, Text, DocumentWritable> 
  {
    public void reduce( Text key, Iterator<DocumentWritable> values, OutputCollector<Text, DocumentWritable> output, Reporter reporter)
      throws IOException
    {
      DocumentWritable doc = new DocumentWritable( );

      while ( values.hasNext( ) )
        {
          doc.merge( values.next( ) );
        }

      output.collect( key, doc );
    }
  }
  
  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(Main.class), new Main(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if ( args.length < 2 )
      {
        System.err.println( "Main <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), Main.class);
    conf.setJobName("Main");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DocumentWritable.class);
    
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    // Choose the outputformat to either merge or index the records
    //   LuceneOutputFormat  - writes to Lucene index.
    //   SolrOutputFormat    - sends documents to external Solr server
    //   MapFileOutputFormat - merges documents into Hadoop MapFile
    //                         org.apache.hadoop.mapred.MapFileOutputFormat
    conf.setOutputFormat( (Class) Class.forName( conf.get( "jbs.outputformat.class", "org.apache.hadoop.mapred.MapFileOutputFormat" ) ) );

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
                MultipleInputs.addInputPath( conf, new Path( args[i] ), TextInputFormat.class, TextMapper.class );
              }
          }
      }

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));
    // FileOutputFormat.setCompressOutput( conf, true );
    
    JobClient.runJob(conf);
    
    return 0;
  }

}
