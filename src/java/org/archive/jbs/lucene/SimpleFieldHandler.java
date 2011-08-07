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

package org.archive.jbs.lucene;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;

import org.archive.jbs.Document;

/**
 * Straightforward implementation of a FieldHandler that gets the
 * field value from the Document via the specified key, then
 * adds it to the Document using the given name, store and index
 * values.
 *
 * NOTE: If a field value is empty, it is not added to the Document.
 */
public class SimpleFieldHandler implements FieldHandler
{
  String name;
  String key;
  Field.Store store;
  Field.Index index;

  public SimpleFieldHandler( String name, Field.Store store, Field.Index index )
  {
    this( name, name, store, index );
  }

  public SimpleFieldHandler( String name, String key, Field.Store store, Field.Index index )
  {
    this.name  = name;
    this.key   = key;
    this.store = store;
    this.index = index;
  }

  public void handle( org.apache.lucene.document.Document luceneDocument, Document document )
  {
    String value = document.get( key ).trim( );
    
    if ( value.length( ) < 1 )
      {
        return ;
      }

    luceneDocument.add( new Field( name, value, store, index ) );
  }

}
