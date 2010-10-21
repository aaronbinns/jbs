
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;


public class TypeHandler implements FieldHandler
{
  TypeNormalizer normalizer;

  public TypeHandler( TypeNormalizer normalizer )
  {
    this.normalizer = normalizer;
  }

  public void handle( Document doc, DocumentProperties properties )
  {
    String type = this.normalizer.normalize( properties );

    // We store and index the normalized type.
    //
    // Alternatives might be:
    //  1. Index normalized type, store original type.
    //  2. Index both normalized and original types, store original.
    doc.add( new Field( "type", type, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
  }

}
