/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright
 * ownership.  The ASF licenses this file to you under the Apache
 * License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This class is derived from Hadoop's 
 *
 *    org.apache.hadoop.mapred.lib.MultipleOutputFormat
 */

package org.archive.jbs.util;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 */
public class PerMapOutputFormat<K,V> extends FileOutputFormat<K,V>
{
  private String getOutputFilename( JobConf job )
    throws IOException
  {
    String regex   = job.get( "permap.regex"  , null );
    String replace = job.get( "permap.replace", null );
    String suffix  = job.get( "permap.suffix" , null );

    String inputFilename = job.get("map.input.file");

    if ( inputFilename == null ) throw new IOException( "map.input.file is null, not runnin in map task?" );

    String outputFilename = (new Path(inputFilename)).getName();

    if ( regex != null && replace != null )
      {
        outputFilename = outputFilename.replaceAll( regex, replace );
      }
    else if ( suffix != null )
      {
        outputFilename += suffix;
      }

    if ( outputFilename == null ) throw new IOException( "outputFilename is null" );

    return outputFilename;
  }

  private OutputFormat<K,V> getOutputFormat( JobConf job )
  {
    return ReflectionUtils.newInstance( job.getClass( "permap.output.format.class",
                                                       SequenceFileOutputFormat.class,
                                                       OutputFormat.class ),
                                        job );
  }
  

  public RecordWriter<K, V> getRecordWriter( FileSystem fs, JobConf job, String name, Progressable progress )
    throws IOException
  {
    String outputFilename = getOutputFilename( job );

    OutputFormat<K,V> of = getOutputFormat( job );

    return of.getRecordWriter( fs, job, outputFilename, progress );
    
  }

  /**
   * Over-ride the default FileOutputFormat's checkOutputSpecs() to
   * allow for the target directory to already exist.
   */
  public void checkOutputSpecs( FileSystem ignored, JobConf job )
    throws FileAlreadyExistsException, InvalidJobConfException, IOException 
  {
  }

}
