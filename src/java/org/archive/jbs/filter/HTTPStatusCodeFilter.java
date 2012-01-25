/*
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
 * Filter documents based on HTTP status code, stored in the "code"
 * property.  By default, allow documents with no "code" property
 * because we didn't start storing it until recently.
 */
public class HTTPStatusCodeFilter implements DocumentFilter
{
  public static final String DEFAULT_RANGE = "200-299";
  
  List<Range> ranges = new ArrayList<Range>( );

  public HTTPStatusCodeFilter( String configuration )
  {
    if ( configuration == null  || "".equals(configuration.trim()) )
      {
        configuration = DEFAULT_RANGE;
      }

    configuration = configuration.trim( );

    for ( String value : configuration.split( "\\s+" ) )
      {
        Range range = new Range( );

        // Special handling for "unknown" where an ARCRecord doesn't have
        // an HTTP status code.  The ARCRecord.getStatusCode() returns
        // -1 in that case, so we make a range for it.
        if ( value.toLowerCase( ).equals( "unknown" ) )
          {
            range.lower = -1;
            range.upper = -1;

            this.ranges.add( range );

            continue;
          }

        String values[] = value.split( "[-]" );

        try
          {
            switch ( values.length )
              {
              case 2:
                // It's a range, N-M
                range.lower = Integer.parseInt( values[0] );
                range.upper = Integer.parseInt( values[1] );
                break;
                
              case 1:
                // It's a single value, convert to a single-value range
                range.lower = Integer.parseInt( values[0] );
                range.upper = range.lower;
                break;
                
              default:
                // Bad format
                throw new RuntimeException( "Illegal format for HTTPStatusCodeFilter: " + range );
              }

            this.ranges.add( range );
          }
        catch ( NumberFormatException nfe )
          {
            throw new RuntimeException( "Illegal format for HTTPStatusCodeFilter: " + range, nfe );
          }
      }

  }

  /**
   * 
   */ 
  public boolean isAllowed( Document document )
  {
    String codeString = document.get( "code" );

    if ( "".equals( codeString ) ) return true;

    try
      {
        int code = Integer.parseInt( codeString );

        // If the code is in any of the valid ranges, allow it.
        for ( Range r : this.ranges )
          {
            return ( r.lower <= code && code <= r.upper );
          }
      }
    catch ( NumberFormatException nfe )
      {
        // Eat it.
      }

    return false;
  }

  static class Range 
  {
    int lower;
    int upper;
  }

}
