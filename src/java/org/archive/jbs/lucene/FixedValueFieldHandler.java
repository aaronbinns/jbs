/*
 * Copyright 2012 Internet Archive
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
 * A field handler that sets the field to a fixed value.
 *
 * NOTE: If a field value is empty, it is not added to the Document.
 */
public class FixedValueFieldHandler implements FieldHandler
{
  String name;
  String value;
  Field.Store store;
  Field.Index index;

  public FixedValueFieldHandler( String name, String value, Field.Store store, Field.Index index )
  {
    this.name  = name;
    this.value = value;
    this.store = store;
    this.index = index;
  }

  public void handle( org.apache.lucene.document.Document luceneDocument, Document document )
  {
    if ( value.length( ) < 1 )
      {
        return ;
      }

    luceneDocument.add( new Field( name, value, store, index ) );
  }

}
