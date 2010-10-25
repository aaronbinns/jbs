

import java.io.*;
import java.net.*;


public class RequiredFieldsFilter implements DocumentFilter
{

  public boolean isAllowed( DocumentProperties properties )
  {
    String url = properties.get( "url"  );
    
    return url.length() > 0;
  }

}
