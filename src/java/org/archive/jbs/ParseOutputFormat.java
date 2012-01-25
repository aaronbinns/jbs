/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.archive.jbs;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;

/**
 * Write map outputs to files named according to their corresponding
 * input files, rather than "part-nnnnn".  E.g. if an input file is
 * "foo.warc.gz" then an output file will be created with the same
 * name.
 */
public class ParseOutputFormat<K,V> extends MultipleSequenceFileOutputFormat<K,V>
{

  /**
   * Ensure that the numOfTrailingLegs property is set, so that the
   * MultipleSequenceFileOutputFormat will use the input filename for
   * the map output filename.
   */
  public void checkOutputSpecs( FileSystem ignored, JobConf job )
    throws FileAlreadyExistsException, InvalidJobConfException, IOException 
  {
    job.setInt( "mapred.outputformat.numOfTrailingLegs", 1 );
  }
  
}