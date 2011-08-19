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
 * Handler that computes a boost value based on any number of values
 * in the input Document, then applies that boost value to the Lucene
 * doc.  The boost value is also stored in the Lucene doc for future
 * reference, such as auditing/debugging/tracking.
 */
public class BoostHandler implements FieldHandler
{

  public void handle( org.apache.lucene.document.Document luceneDocument, Document document )
  {
    long numInlinks = getLong( document, "numInlinks", 0 );

    if ( numInlinks > 10 )
      {
        float boost = (float) Math.log10( numInlinks );
        
        // Store the boost value with the document.  This can be handy
        // for debugging/auditing, but it is not searched on, nor does
        // it actually have any operational effect.  It's just so that
        // we can know later what boost value we set on the document.
        // There's no other way to tell.
        luceneDocument.add( new Field( "boost", Float.toString(boost), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );

        luceneDocument.setBoost( boost );
      }
  }

  public long getLong( Document document, String key, long defaultValue )
  {
    long value = defaultValue;
    try
      {
        value = Long.parseLong( document.get( key ) );
      }
    catch ( NumberFormatException nfe )
      {
        // Skip it
      }
    return value;
  }

}
