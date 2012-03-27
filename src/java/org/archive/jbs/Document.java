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
import org.json.JSONTokener;

/**
 * Document for use with Hadoop and full-text processing and analysis.
 * Serializable via JSON.
 *
 * Properties are simple key/value pairs, where values can be single
 * or multi-valued.  Multiple values are kept in a set, so only unique
 * values are retained.  Setting a property to 'null', "", an empty
 * collection or a collection containing only 'null'/"" will 
 * remove the property from the Document.
 *
 * Getting property values always returns "" or an empty collection if
 * the property does not exist.
 */
public class Document
{
  private Map<String,Object> properties;
  private ArrayList<Link>    links;

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
    throws IOException
  {
    this();

    fromJSON( json );
  }

  /**
   * Construct a document from a JSON string via a Reader.
   */
  public Document( Reader r )
    throws IOException
  {
    this();

    try
      {
        fromJSON( new JSONObject( new JSONTokener( r ) ) );
      }
    catch ( JSONException jse )
      {
        throw new IOException( jse );
      }
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

    if ( value == null || value.length() == 0 )
      {
        properties.remove( key );

        return ;
      }

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
        properties.remove( key );
        return ;
      }

    Set<String> newValues = new HashSet<String>( c.size( ) );
    for ( String newValue : c )
      {
        if ( newValue != null )
          {
            newValue = newValue.trim();
            
            if ( newValue.length() > 0 )
              {
                newValues.add( newValue );
              }
          }
      }

    // If c contained only 'null' elements, then at this point it
    // would be empty.  In that case we just set a 'null' value for
    // the property.
    if ( newValues.size( ) == 0 )
      {
        properties.remove( key );
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
        
        if ( newValue.length() == 0 ) return;
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

            if ( newValue.length() > 0 )
              {
                newValues.add( newValue );
              }
          }
      }

    // If after uniquing the new values and removing any 'null' and ""
    // values, if the set is empty, then there's nothing to add.
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
    if ( url  == null ) url  = "";
    if ( text == null ) text = "";

    url  = url.trim();
    text = text.trim();

    this.links.add( new Link( url, text ) );
  }
  
  /**
   * Get the list of Links.
   */
  public List<Link> getLinks( )
  {
    return (List<Link>) this.links.clone( );
  }

  /**
   * Remove all the links.
   */
  public void clearLinks( )
  {
    this.links.clear( );
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
                    // Even though the set() and add() methods should
                    // enforce that no value is "", we leave this
                    // check here as a belt-and-suspenders thing.
                    if ( s.length() != 0 )
                      {
                        json.accumulate( key, s );
                      }
                  }
              }
            else
              {
                String s = (String) value;

                // Even though the set() and add() methods should
                // enforce that no value is "", we leave this
                // check here as a belt-and-suspenders thing.
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

            // The link text is frequently "", we just don't bother
            // serializing it out, to save space.
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
   * Initialize self from given JSONObject
   */
  private void fromJSON( JSONObject json )
    throws IOException
  {
    try
      {
        String[] names = JSONObject.getNames( json );

        // If no names, then empty object.
        if ( names == null ) return ;

        for ( String name : names )
          {
            name = name.trim();

            Object o = json.get( name );

            if ( o instanceof String  ||
                 o instanceof Long    ||
                 o instanceof Double  ||
                 o instanceof Integer ||
                 o instanceof Boolean )
              {
                this.add( name, o.toString() );
              }
            else if ( o instanceof JSONArray )
              {
                JSONArray values = (JSONArray) o;
                
                for ( int i = 0; i < values.length() ; i++ )
                  {
                    Object value = values.get(i);
                    
                    if ( "outlinks".equals(name) )
                      {
                        // Hrm, it should be an object.
                        if ( ! (value instanceof JSONObject) ) continue;
                        
                        JSONObject link = (JSONObject) value;
                        
                        // Use optString() to get "" rather than 'null' if there is no value.
                        this.addLink( link.optString( "url" ), link.optString( "text" ) );
                      }
                    else
                      {
                        // For any multi-valued property (other than "outlinks") assume that it
                        // is an array of Strings.  So just cast to String here.
                        this.add( name, (String) value );
                      }
                  }
              }
          }
      }
    catch ( JSONException jse )
      {
        throw new IOException( jse );
      }
  }

  /**
   * Initialize self from given JSON string
   */
  private void fromJSON( String s )
    throws IOException
  {
    try
      {
        JSONObject json = new JSONObject( s );
        
        fromJSON( json );
      }
    catch ( JSONException jse )
      {
        throw new IOException( jse );
      }
  }
  
  /**
   * A link is a URL and the link text, a.k.a. "anchor text".
   */
  public static class Link
  {
    private String url;
    private String text;

    private Link( String url, String text )
    {
      this.url  = url;
      this.text = text;
    }

    public String getUrl( ) { return url; }
    public String getText( ) { return text; }
  }

}
