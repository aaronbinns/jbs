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
