
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * Handy utility class to wrap a MapWritable so it can be used with
 * DocumentFilters and stuff.
 */
public class MapWritableAdapter implements DocumentProperties
{
  MapWritable map;
  Text textKey;
  
  public MapWritableAdapter( MapWritable map )
  {
    this.map = map;
    this.textKey = new Text();
  }
  
  public String get( String key )
  {
    this.textKey.set( key );
    
    Text value = (Text) this.map.get( this.textKey );
    
    if ( value == null ) return "";
    
    return value.toString().trim();
  }
  
}
