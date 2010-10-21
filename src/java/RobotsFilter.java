

import java.io.*;
import java.net.*;


public class RobotsFilter implements DocumentFilter
{

  public boolean isAllowed( DocumentProperties properties )
  {
    String type = properties.get( "type" );
    String url  = properties.get( "url"  );
    
    try
      {
        URI uri = new URI( url );
        if ( "text/plain".equals( type ) && "/robots.txt".equals( uri.getPath() ) )
          {
            return false;
          }
      } catch ( URISyntaxException e ) { }
    
    return true;
  }

}
