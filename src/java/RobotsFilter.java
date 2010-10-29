

import java.io.*;
import java.net.*;


public class RobotsFilter implements DocumentFilter
{

  public boolean isAllowed( DocumentProperties properties )
  {
    String url  = properties.get( "url"  );
    
    try
      {
        URI uri = new URI( url );
        
        String path = uri.getPath().trim( );

        if ( "/favicon.ico".equals( path ) ||
             "/robots.txt" .equals( path ) )
          {
            return false;
          }
      } catch ( URISyntaxException e ) { }
    
    return true;
  }

}
