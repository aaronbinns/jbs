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
import java.util.*;
import java.net.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.metadata.Metadata;

import com.cybozu.labs.langdetect.*;

/**
 * <p>
 * </p>
 * <p>
 * </p>
 * <p>
 * </p>
 * <p>
 *   <strong>NOTE:</strong> This class is currently experimental and
 *   is not used in production.  If it's ultimately not useful, it will
 *   likely be removed.
 * </p>
 */
public class LanguageIdent extends Configured implements Tool
{
  public static String getProfilesDirectory( String name )
  {
    URL url = LanguageIdent.class.getClassLoader().getResource( name ); 
    
    System.out.println( "u = " + url.toString( ) );
    System.out.println( "u = " + url.getPath( ) );

    if ( "jar".equals(url.getProtocol()) ) 
      {
        try
          {
            java.net.JarURLConnection connection = (java.net.JarURLConnection) url.openConnection();
            URL url2 = connection.getJarFileURL();
            if ( !"file".equals(url2.getProtocol()) )
              {
                System.out.println( "Jar file is not a file: " + url2 );
                return  null;
              }
            
            String directory = (new File( url2.getFile( ) )).getParent( ) + "/" + name;

            System.out.println( "profiles directory = " + directory );
            
            return directory;
          }
        catch ( IOException ioe )
          {
            System.out.println( ioe );
            return null;
          }
      }
    else
      {
        System.out.println( "Not a jar, using: " + url.getPath() );
        return url.getPath();
      }
  }

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, Text>
  {
    public Map( )
    {
      try
        {
          DetectorFactory.loadProfile( getProfilesDirectory( "lib/profiles" ) );
        }
      catch ( LangDetectException lde )
        {
          System.err.println( lde );
        }
    }

    public void map( Text key, Writable value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      String text = "";

      if ( value instanceof ParseData )
        {
          return ;
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

      // TODO
      try
        {
          Detector detector = DetectorFactory.create( 0.5 );

          detector.append(text);

          /* for ( Language lang : detector.getProbabilities( ) )
            {
              output.collect( key, new Text( lang.toString( ) ) );
            }
          */
          output.collect( key, new Text( detector.getProbabilities( ).toString() ) );
        }
      catch ( LangDetectException lde )
        {
          
        }
    }
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
  {
    public Reduce( )
    {
      try
        {
          DetectorFactory.loadProfile( getProfilesDirectory( "lib/profiles" ) );
        }
      catch ( LangDetectException lde )
        {
          System.err.println( lde );
        }
    }

    public void reduce( Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException
    {
      if ( values.hasNext( ) )
        {
          output.collect( key, values.next( ) );
        }
    }
  }
  
  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(LanguageIdent.class), new LanguageIdent(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    if (args.length < 2)
      {
        System.err.println( "LanguageIdent <output> <input>..." );
        return 1;
      }
      
    JobConf conf = new JobConf( getConf(), LanguageIdent.class);
    conf.setJobName("LanguageIdent");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    
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
            // No need to process the metadata, just the page contents.
            // MultipleInputs.addInputPath( conf, new Path( p, "parse_data" ), SequenceFileInputFormat.class, Map.class );
            MultipleInputs.addInputPath( conf, new Path( p, "parse_text" ), SequenceFileInputFormat.class, Map.class );
          }
      }

    FileOutputFormat.setOutputPath(conf, new Path(args[0]));

    JobClient.runJob(conf);
    
    return 0;
  }

}
