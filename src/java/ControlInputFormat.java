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

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;

/**
 * <p>Hadoop InputFormat implementation that reads a Text key from an
 * input file and returns the key paired with a token rather than the
 * value stored in the file.  That is:
 * <code>
 *   &lt;key1, TRUE&gt;
 *   &lt;key2, TRUE&gt;
 *   &lt;key3, TRUE&gt;
 *   &lt;key4, TRUE&gt;
 *   ...
 * </code></p>
 * <p>In practice, we might be able to use this for performing
 * de-duplication via Hadoop.  If we have an existing NutchWAX
 * segment (or similar) containing the existing records, then
 * when we compare a new WARC file's records to find duplicates,
 * we don't need the entire record in the segment, just the
 * key.</p>
 * <p>This ControlInputFormat class could be used to read the existing
 * segment, returning just the keys paired with an insignificant
 * token.  The keys could be combined via the MapReduce framework with
 * the K-V pairs in the newly added WARC and duplicates could be found
 * and/or eliminated.</p>
 * <p><strong>NOTE:</strong> This class is currently experimental and
 * is not used in production.  If it's ultimately not useful, it will
 * likely be removed.</p>
 */
public class ControlInputFormat extends SequenceFileInputFormat<Text,GenericWritable>
{
  public static class ControlMarker extends BooleanWritable
  {
    public ControlMarker( )
    {
      super(true);
    }
  }

  public static final ControlMarker MARKER = new ControlMarker( );

  private boolean isControl( Path p )
  {
    return false;
  }
  
  public RecordReader<Text,GenericWritable> getRecordReader( final InputSplit split,
                                                             final JobConf    job, 
                                                             final Reporter   reporter )
    throws IOException
  {
    reporter.setStatus( split.toString( ) );
    
    final FileSplit fSplit = (FileSplit) split;

    final SequenceFile.Reader reader =
      new SequenceFile.Reader( FileSystem.get(job), fSplit.getPath(), job );
    
    final GenericWritable w;
    try
      {
        w = (GenericWritable) reader.getValueClass().newInstance();
      }
    catch (Exception e)
      {
        throw new IOException(e.toString());
      }
    
    try 
      {
        return new SequenceFileRecordReader<Text,GenericWritable>(job, fSplit)
          {
            public synchronized boolean next( Text key, GenericWritable value ) throws IOException 
            {
              //System.out.println( "SequenceFileRecordReader.next( " + key + ", " + value + " )" );
              //System.out.println( "  path: " + fSplit.getPath( ) );
              
              boolean res = reader.next(key, w);
              if ( isControl( fSplit.getPath( ) ) )
                {
                  value.set( MARKER );
                }
              else
                {
                  value.set( w.get( ) );
                }
              return res;
            }
          
            public synchronized void close() throws IOException 
            {
              reader.close();
            }
            
            public GenericWritable createValue()
            {
              final GenericWritable w;
              try
                {
                  w = (GenericWritable) reader.getValueClass().newInstance();
                }
              catch (Exception e)
                {
                  throw new RuntimeException(e.toString());
                }
              return w;
            }
            
          };
      }
    catch (IOException e) 
      {
        throw new RuntimeException("Cannot create RecordReader: ", e);
      }
  }
  
}
