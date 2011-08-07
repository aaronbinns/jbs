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

package org.archive.jbs.filter;

import java.io.*;
import java.util.*;

import org.archive.jbs.Document;

/**
 * Simple DocumentFilter which only allows documents with specific
 * types.  Types are normalized before they are checked.  A default
 * list is provided based on IA experience.
 *
 * If an empty list is specified, then all types are allowed.
 */
public class TypeFilter implements DocumentFilter
{
  public static final String[] DEFAULT_ALLOWED = 
    {
      "text/html", 
      "text/plain",
      "application/pdf", 
      // MS Office document types
      "application/msword", 
      "application/vnd.ms-powerpoint", 
      // OpenOffice document types
      "application/vnd.oasis.opendocument.text",
      "application/vnd.oasis.opendocument.presentation",
      "application/vnd.oasis.opendocument.spreadsheet",
    };

  private Set<String>    allowed;
  private TypeNormalizer normalizer;

  public TypeFilter( )
  {
    
  }

  public TypeFilter( Set<String> allowed, TypeNormalizer normalizer )
  {
    this.allowed    = allowed;
    this.normalizer = normalizer;
  }

  public void setTypeNormalizer( TypeNormalizer normalizer )
  {
    this.normalizer = normalizer;
  }

  public void setAllowed( Set<String> allowed )
  {
    this.allowed = allowed;
  }

  public Set<String> getAllowed( )
  {
    return this.allowed;
  }
  
  public boolean isAllowed( Document document )
  {
    String type = document.get( "type" );

    // If no explicit list of allowed types, allow them all.
    if ( this.allowed == null || this.allowed.size( ) == 0 )
      {
        return true;
      }

    // De-alias it.
    type = this.normalizer.normalize( document );

    return allowed.contains( type );
  }
  
  public static Set<String> parse( String s )
  {
    Set<String> types = new HashSet<String>( );

    for ( String type : s.split( "\\s+" ) )
      {
        if ( type.length() < 1 ) continue ;

        types.add( type );
      }

    return types;
  }

  public static Set<String> getDefaultAllowed( )
  {
    Set<String> defaults = new HashSet<String>( );
    
    for ( String allowed : DEFAULT_ALLOWED )
      {
        defaults.add( allowed );
      }

    return defaults;
  }

}
