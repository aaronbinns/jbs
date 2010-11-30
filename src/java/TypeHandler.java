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

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;

/**
 * Custom FieldHandler implementation for type.
 *
 * First, the type is normalized, then it is stored and is indexed as
 * a single token.
 */
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
