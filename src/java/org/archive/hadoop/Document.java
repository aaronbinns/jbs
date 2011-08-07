
package org.archive.hadoop;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;

/**
 * Document for use with Hadoop and full-text processing and analysis.
 * Serializable via Hadoop Writable interface.
 * Properties are simple key/value pairs, where values can be single
 * or multi-valued.  Multiple values are kept in a set, so only unique
 * values are retained.  Setting a property to 'null' or a collection
 * containing a single 'null' element sets the property to 'null.
 */
public class Document implements Writable
{
  public Map<String,Object> properties;
  public ArrayList<Link>    links;

  /**
   *
   */
  public Document( )
  {
    // At present, there are typicaly 12 properties.
    properties = new HashMap<String,Object>( 12 );
    links      = new ArrayList<Link>();
  }

  /**
   * Get the String value for the property key.  If the property has
   * multiple values, get one of them.
   */
  public String get( String key )
  {
    Object value = properties.get( key );

    if ( value == null ) return null;

    if ( value instanceof Set )
      {
        Set<String> values = (Set<String>) value;

        if ( values.size() > 0 )
          {
            return values.iterator().next();
          }
      }
    else
      {
        return (String) value;
      }

    return null;
  }
  
  /**
   * Get all the String values for the property key.  If the property
   * has one value then a singleton Set<String> is returned.
   */
  public Set<String> getAll( String key )
  {
    Object value = properties.get( key );
    
    if ( value == null ) return Collections.emptySet();

    if ( value instanceof Set )
      {
        return Collections.unmodifiableSet( (Set<String>) value );
      }
    else
      {
        return Collections.singleton( (String) value );
      }
  }

  /**
   * Set the property to the key/value pair.
   */
  public void set( String key, String value )
  {
    properties.put( key, value );
  }

  /**
   * Set the property to the values.  If the values is a null
   * collection, or only contains one element which is null, then it's
   * the same as set(key,null).
   */
  public void set( String key, Collection<String> newValues )
  {
    if ( newValues == null || newValues.size( ) == 0 )
      {
        properties.put( key, null );
        return ;
      }

    if ( newValues.size( ) == 1 )
      {
        // Get the 1 element of the collection.
        properties.put( key, newValues.iterator( ).next( ) );
        return;
      }

    Set<String> values;

    Object value = properties.get( key );
    if ( value != null && value instanceof Set )
      {
        values = (Set<String>) value;
        values.clear();
      }
    else
      {
        values = new HashSet<String>( newValues.size() );
      }

    values.addAll( newValues );

    // If the resulting set of values contains only 1 value then set
    // the property value to it, rather than a Set with 1 element.
    if ( values.size( ) == 1 )
      {
        properties.put( key, values.iterator( ).next( ) );
        return ;
      }

    properties.put( key, values );
  }

  /**
   * Add value to the property key.
   */
  public void add( String key, String newValue )
  {
    if ( newValue == null ) return;

    Object value = properties.get( key );

    if ( value == null )
      {
        properties.put( key, value );
        
        return ;
      }

    Set<String> values;

    if ( value instanceof String )
      {
        // If we're adding the same value, ignore it.
        if ( ((String) value) == newValue )
          {
            return ;
          }

        // Create a Set for multi-values and seed it with the existing
        // value.
        values = new HashSet<String>( 2 );
        values.add( (String) value );
        properties.put( key, values );
      }
    else
      {
        values = (Set<String>) value;
      }

    // Finally add the new value to the Set<String>.
    values.add( newValue );
  }

  /**
   * Add collection of new values to the property key.
   */
  public void add( String key, Collection<String> c )
  {
    if ( c == null || c.size( ) == 0 )
      {
        return;
      }

    Set<String> newValues  = new HashSet<String>( c );
    newValues.remove( null );

    // If after uniquing the new values and removing any 'null', the
    // set is empty, then there's nothing to add.
    if ( newValues.size( ) == 0 )
      {
        return;
      }

    // If there's only 1 new element to add, then use add(String,String)
    if ( newValues.size( ) == 1 )
      {
        add( key, newValues.iterator( ).next( ) );
        return ;
      }

    Object value = properties.get( key );
    
    // If there is no existing value then just set the property to be
    // the newValues.  At this point, we know that the newValues has
    // at least 2 non-null elements.
    if ( value == null )
      { 
        properties.put( key, newValues );
        return ;
      }
    
    Set<String> values;

    if ( value instanceof String )
      {
        values = new HashSet<String>( newValues );
        values.add( (String) value );
        properties.put( key, values );
      }
    else
      {
        values = (Set<String>) value;
        values.addAll( newValues );
      }
  }

  /**
   *
   */
  public void addLink( String url, String text )
  {
    this.links.add( new Link( url, text ) );
  }
  
  /**
   *
   */
  public List<Link> getLinks( )
  {
    return Collections.unmodifiableList( links );
  }

  /**
   * Merge the other Document into this one.  Since document contents
   * are unique, the only values which need to be actually merged are
   * the non-content metadata, such as the (re)visit dates and the
   * collection.  Those fields are not related to the contents of the
   * archived web page, and therefore can vary from one instance of
   * this unique document to another.  Those fields are the ones that
   * are merged together by this method.  The rest, such as title, and
   * mime-type are assumed to be the same and are ignored by the
   * merge.
   */
  public void merge( Document other )
  {
    
  }

  /**
   * Hadoop Writable serialization
   */
  public void write( DataOutput out )
    throws IOException
  {
    /* TODO: Is this worth it?
    // First, remove all properties with a null value.
    for ( Map.Entry<String,Object> e : properties.entrySet( ) )
      {
        if ( e.getKey( ) == null || e.getValue( ) == null )
          {
            properties.remove( e.getKey( ) );
          }
      }
    */

    // First write out how many properties there are.  We'll need this
    // for when we read them back in.
    out.writeInt( properties.size() );

    for ( Map.Entry<String,Object> e : properties.entrySet() )
      {
        String key   = e.getKey( );
        Object value = e.getValue( );
        
        Text.writeString( out, e.getKey() );

        if ( value instanceof String )
          {
            out.writeInt( 1 );
            Text.writeString( out, (String) value );
          }
        else
          {
            Set<String> values = (Set<String>) value;
            out.writeInt( values.size( ) );
            for ( String s : values )
              {
                Text.writeString( out, s );
              }
          }
      }

    // Write out the length of the list of links, then all the links
    out.writeInt( links.size() );
    for ( Link o : links )
      {
        Text.writeString( out, o.getUrl () );
        Text.writeString( out, o.getText() );
      }
  }

  /**
   * Hadoop Writable serialization
   */
  public void readFields( DataInput in )
    throws IOException
  {
    properties.clear();
    links.clear();

    int num = in.readInt();
    for ( int i = 0 ; i < num ; i++ )
      {
        String key = Text.readString( in );

        int numVals = in.readInt( );
        for ( int j = 0 ; j < numVals ; j++ )
          {
            String value = Text.readString( in );
            add( key, value );
          }
      }

    num = in.readInt();
    links.ensureCapacity( num );
    for ( int i = 0 ; i < num ; i++ )
      {
        links.add( new Link( Text.readString( in ) , Text.readString( in ) ) );
      }
  }

  public static class Link
  {
    String url;
    String text;

    public Link( String url, String text )
    {
      this.url  = url;
      this.text = text;
    }

    public String getUrl( ) { return url; }
    public void setUrl( String url ) { this.url = url; }

    public String getText( ) { return text; }
    public void setText( String text ) { this.text = text; }
  }

}
