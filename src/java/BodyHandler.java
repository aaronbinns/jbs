
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;


public class BodyHandler implements FieldHandler
{

  public void handle( Document doc, DocumentProperties properties )
  {
    // Special handling for content field.
    String content = properties.get( "content_parsed" );
    if ( content.length() > 0 )
      {
        doc.add( new Field( "content", content, Field.Store.NO, Field.Index.ANALYZED ) );

        byte[] compressed = CompressionTools.compressString( content );

        // Store the shorter of the two.
        if ( compressed.length < content.length() )
          {
            doc.add( new Field( "content", compressed, Field.Store.YES ) );    
          }
        else
          {
            doc.add( new Field( "content", content, Field.Store.YES, Field.Index.NO ) );
          }
      }
  }

}
