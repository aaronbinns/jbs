
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * Implementors take the given MapWritable, create a document from it
 * and write it to an index.
 */
public interface DocumentWriter
{
  public void setFilter( String name, DocumentFilter filter );

  public DocumentFilter getFilter( String name );

  public void add( Text key, MapWritable properties ) throws IOException;

}
