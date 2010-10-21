
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;


public class SiteHandler implements FieldHandler
{

  public void handle( Document doc, DocumentProperties properties )
  {
    // Special handling for site
    try
      {
        String url = properties.get( "url" );

        String site = (new URL( url)).getHost( );

        // Strip off any "www[0-9]*." header.
        site = site.toLowerCase().replaceFirst( "^www[0-9]*[.]", "" );

        // Special rule for Photobucket
        site = site.replaceAll( "^[a-z0-9]+.photobucket.com$", "photobucket.com" );

        doc.add( new Field( "site", site, Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS) );
      }
    catch ( MalformedURLException mue )
      {
        // Rut-roh.
      }
  }

}
