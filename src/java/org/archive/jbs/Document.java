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

package org.archive.jbs;

import java.io.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

/**
 * Document for use with Hadoop and full-text processing and analysis.
 * Serializable via JSON.
 *
 * Properties are simple key/value pairs, where values can be single
 * or multi-valued.  Multiple values are kept in a set, so only unique
 * values are retained.  Setting a property to 'null' or a collection
 * containing a single 'null' element sets the property to 'null.
 */
public class Document
{
  public Map<String,Object> properties;
  public ArrayList<Link>    links;

  /**
   * Construct and empty document.
   */
  public Document( )
  {
    // At present, there are typicaly 12 properties.
    properties = new HashMap<String,Object>( 12 );
    links      = new ArrayList<Link>();
  }

  /**
   * Construct a document from a JSON string.
   */
  public Document( String json )
  {
    this();

    fromJSON( json );
  }

  /**
   * Get the String value for the property key.  If the property has
   * multiple values, get one of them.  If the property doesn't exist
   * or has no value, return "" rather than null.
   */
  public String get( String key )
  {
    Object value = properties.get( key );

    if ( value == null ) return "";

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

    return "";
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
    if ( value != null ) value = value.trim();

    properties.put( key, value );
  }

  /**
   * Set the property to the values.  If the values is a null
   * collection, or only contains one element which is null, then it's
   * the same as set(key,null).
   */
  public void set( String key, Collection<String> c )
  {
    if ( c == null || c.size() == 0 )
      {
        properties.put( key, null );
        return ;
      }

    Set<String> newValues = new HashSet<String>( c.size( ) );
    for ( String newValue : c )
      {
        if ( newValue != null )
          {
            newValues.add( newValue.trim( ) );
          }
      }

    // If c contained only 'null' elements, then at this point it
    // would be empty.  In that case we just set a 'null' value for
    // the property.
    if ( newValues.size( ) == 0 )
      {
        properties.put( key, null );
      }

    if ( newValues.size( ) == 1 )
      {
        // Get the 1 element of the collection.
        properties.put( key, newValues.iterator( ).next( ) );
        return;
      }

    properties.put( key, newValues );
  }

  /**
   * Add value to the property key.
   */
  public void add( String key, String newValue )
  {
    if ( newValue == null ) 
      {
        return;
      }
    else
      {
        newValue = newValue.trim();
      }

    Object value = properties.get( key );

    if ( value == null )
      {
        properties.put( key, newValue );
        
        return ;
      }

    Set<String> values;

    if ( value instanceof String )
      {
        // If we're adding the same value, ignore it.
        if ( ((String) value).equals( newValue ) )
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

    Set<String> newValues = new HashSet<String>( c.size( ) );
    for ( String newValue : c )
      {
        if ( newValue != null )
          {
            newValue = newValue.trim();
            newValues.add( newValue );
          }
      }

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
   * Add a link.
   */
  public void addLink( String url, String text )
  {
    if ( text != null )
      {
        text = text.trim();
      }
    else
      {
        text = "";
      }

    this.links.add( new Link( url, text ) );
  }
  
  /**
   * Get the list of Links.
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
    for ( String key : other.properties.keySet( ) )
      {
        this.add( key, other.getAll( key ) );
      }

    // FIXME: Is there something smarter to do here?  We take whoever
    // has non-zero list of links.
    if ( links.size() == 0 && other.links.size( ) > 0 )
      {
        links.addAll( other.links );
      }
  }

  /**
   * Serialize out as a JSON string.
   */
  public String toString( )
  {
    try
      {
        JSONObject json = new JSONObject( );
        
        for ( Map.Entry<String,Object> e : properties.entrySet() )
          {
            String key   = e.getKey( );
            Object value = e.getValue( );
            
            // Skip any property with a null value.
            if ( value == null ) continue;

            // Handle the outlinks later.
            if ( "outlinks".equals( key ) ) continue;
            
            if ( value instanceof Set )
              {
                Set<String> values = (Set<String>) value;
                
                for ( String s : values )
                  {
                    if ( s.length() != 0 )
                      {
                        json.accumulate( key, s );
                      }
                  }
              }
            else
              {
                String s = (String) value;

                if ( s.length() != 0 )
                  {
                    json.accumulate( key, (String) value );
                  }
              }
          }
        
        // Write the links
        for ( Link link : links )
          {
            JSONObject jlink = new JSONObject( );

            String url = link.getUrl( );
            if ( url == null || url.length() == 0 ) continue;

            jlink.put( "url" , url );

            String text = link.getText();
            if ( text.length() != 0 )
              {
                jlink.put( "text", text );
              }
            
            json.append( "outlinks", jlink );
          }
        
        return json.toString();
      }
    catch ( JSONException jse )
      {
        throw new RuntimeException( jse );
      }
  }

  /**
   *
   */
  private void fromJSON( String s )
  {
    try
      {
        JSONObject json = new JSONObject( s );
        
        String[] names = JSONObject.getNames( json );

        // If no names, then empty object.
        if ( names == null ) return ;

        for ( String name : names )
          {
            name = name.trim();

            // Handle links separately.
            if ( "outlinks".equals( name ) ) continue;

            String value = json.getString( name );
            
            this.add( name, value );
          }

        Object o = json.opt( "outlinks" );
        if ( (o != null) && (o instanceof JSONArray) )
          {
            JSONArray a = (JSONArray) o;

            for ( int i = 0; i < a.length() ; i++ )
              {
                Object l = a.get(i);
                
                // Hrm, it should be an object.
                if ( ! (l instanceof JSONObject) ) continue;

                JSONObject jlink = (JSONObject) l;

                this.addLink( jlink.optString( "url" ), jlink.optString( "text" ) );
              }
          }
      }
    catch ( JSONException jse )
      {
        throw new RuntimeException( jse );
      }
  }
  
  /**
   * Hadoop  serialization
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
   */

  public static class Link
  {
    private String url;
    private String text;

    public Link( String url, String text )
    {
      this.url  = url;
      this.text = text == null ? "" : text.trim() ;
    }

    public String getUrl( ) { return url; }
    public void setUrl( String url ) { this.url = url.trim(); }

    public String getText( ) { return text; }
    public void setText( String text ) { this.text = text == null ? "" : text.trim(); }
  }

}
