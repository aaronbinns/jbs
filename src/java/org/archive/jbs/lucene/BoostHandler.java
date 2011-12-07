/*
 * Copyright 2011 Internet Archive
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

package org.archive.jbs.lucene;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;

import org.archive.jbs.Document;

/**
 * Handler that applies a boost value to the Lucene doc.  The boost
 * value is also stored in the Lucene doc for future reference, such
 * as auditing/debugging/tracking.
 */
public class BoostHandler implements FieldHandler
{

  public void handle( org.apache.lucene.document.Document luceneDocument, Document document )
  {
    float boost = getFloat( document, "boost", 1.0F );

    if ( boost != 1.0 )
      {
        // Store the boost value with the document.  This can be handy
        // for debugging/auditing, but it is not searched on, nor does
        // it actually have any operational effect.  It's just so that
        // we can know later what boost value we set on the document.
        // There's no other way to tell.
        luceneDocument.add( new Field( "boost", Float.toString(boost), Field.Store.YES, Field.Index.NO ) );

        luceneDocument.setBoost( boost );
      }
  }

  public float getFloat( Document document, String key, float defaultValue )
  {
    float value = defaultValue;
    try
      {
        value = Float.parseFloat( document.get( key ) );
      }
    catch ( NumberFormatException nfe )
      {
        // Skip it
      }
    return value;
  }

}
