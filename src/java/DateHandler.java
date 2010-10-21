
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;


public class DateHandler implements FieldHandler
{

  public void handle( Document doc, DocumentProperties properties )
  {
    // Special handling for dates.
    HashSet<String> dates = new HashSet<String>( Arrays.asList( properties.get("date").split( "\\s+" ) ) );
    
    for ( String date : dates )
      {
        if ( date.length() == "yyyymmddhhmmss".length( ) )
          {
            // Store, but do not index, the full 14-character date.
            doc.add( new Field( "date", date,                   Field.Store.YES, Field.Index.NO  ) );
            
            // Index, but do not store, the year and the year+month.  These are what can be searched.
            doc.add( new Field( "date", date.substring( 0, 4 ), Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
            doc.add( new Field( "date", date.substring( 0, 6 ), Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
          }
      }
  }

}
