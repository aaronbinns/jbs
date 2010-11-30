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
              System.out.println( "SequenceFileRecordReader.next( " + key + ", " + value + " )" );
              System.out.println( "  path: " + fSplit.getPath( ) );
              
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
