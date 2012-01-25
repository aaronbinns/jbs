
package org.archive.jbs;

import java.io.*;
import java.util.*;

/**
 * This should all be moved into some sort of filtering plugin.
 * Unfortunately the URLFilter plugin interface isn't adequate as it
 * only looks at a URL string.  Rather than jamming a response code
 * through that interface, we do a one-off filter class here.
 *
 * A long-term solution would be to create a new Nutch extension point
 * interface that takes an ARCRecord rather than a URL string.  That
 * way we can write filters that can operate on any part of an
 * ARCRecord, not just the URL.
 */
class HTTPStatusCodeFilter
{
  List<Range> ranges = new ArrayList<Range>( );

  public HTTPStatusCodeFilter( String configuration )
  {
    if ( configuration == null )
      {
        return ;
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
                Parse.LOG.warn( "Illegal format for nutchwax.filter.http.status: " + range );
                continue ;
              }

            this.ranges.add( range );
          }
        catch ( NumberFormatException nfe )
          {
            Parse.LOG.warn( "Illegal format for nutchwax.filter.http.status: " + range, nfe );
          }
      }

  }

  public boolean isAllowed( int code )
  {
    for ( Range r : this.ranges )
      {
          return ( r.lower <= code && code <= r.upper );
      }

    return false;
  }

  static class Range 
  {
    int lower;
    int upper;
  }

}
