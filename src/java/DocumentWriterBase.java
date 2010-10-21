
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * Sub-classes take the given MapWritable, create a document from it
 * and write it to an index.
 */
public abstract class DocumentWriterBase implements DocumentWriter
{
  protected Map<String,DocumentFilter> filters = new HashMap<String,DocumentFilter>( );
  protected TypeNormalizer typeNormalizer;

  public DocumentFilter getFilter( String name )
  {
    return this.filters.get( name );
  }

  public void setFilter( String name, DocumentFilter filter )
  {
    this.filters.put( name, filter );
  }

  public void add( Text key, MapWritable properties ) throws IOException
  {
    this.add( key.toString(), new MapWritableAdapter( properties ) );
  }
  
  public abstract void add( String key, DocumentProperties properties ) throws IOException;
}
