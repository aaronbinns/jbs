/*
 * Copyright 2011 Internet Archive
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

package org.archive.jbs.tools;

import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Command-line utility to dump the contents of a Hadoop MapFile or
 * SequenceFile.
 */
public class Dump extends Configured implements Tool
{

  public static void main(String[] args) throws Exception
  {
    int result = ToolRunner.run( new JobConf(Dump.class), new Dump(), args );

    System.exit( result );
  }

  public int run( String[] args ) throws Exception
  {
    String usage = "Usage: Dump <mapfile|sequencefile>";
      
    if (args.length != 1)
      {
        System.err.println(usage);
        return 1;
      }
      
    String in = args[0];

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    MapFile     .Reader mapReader = null;
    SequenceFile.Reader seqReader = null;
    try
      {
        mapReader = new MapFile.Reader(fs, in, conf);
      }
    catch ( IOException ioe )
      {
        // Hrm, try a sequence file...
      }

    if ( mapReader != null )
      {
        WritableComparable key   = (WritableComparable) ReflectionUtils.newInstance(mapReader.getKeyClass()  , conf);
        Writable           value = (Writable)           ReflectionUtils.newInstance(mapReader.getValueClass(), conf);

        while ( mapReader.next(key, value))
          {
            System.out.println( "[" + key + "] [" + value + "]" );
          }
      }
    else
      {
        // Not a MapFile...try a SequenceFile.
        try
          {
            seqReader = new SequenceFile.Reader(fs, new Path(in), conf);
          }
        catch ( IOException ioe )
          {
            // Hrm, neither MapFile nor SequenceFile.
            throw new IOException( "Cannot open file: " + in );
          }

        WritableComparable key   = (WritableComparable) ReflectionUtils.newInstance(seqReader.getKeyClass()  , conf);
        Writable           value = (Writable)           ReflectionUtils.newInstance(seqReader.getValueClass(), conf);

        while ( seqReader.next(key, value))
          {
            System.out.println( "[" + key + "] [" + value + "]" );
          }
      }

    return 0;
  }
}
