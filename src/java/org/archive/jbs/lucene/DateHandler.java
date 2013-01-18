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

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;

import org.archive.jbs.Document;

/**
 * Custom FieldHandler implementation for dates.
 *
 * The date information in the Document is stored as a
 * whitespace-separated list of 14-digit date values.  That value is
 * tokenized, and converted to a list of date values.
 *
 * The full date value is stored, but not indexed.  Then the date is
 * added as a Lucene field multiple times, with different precisions:
 * YYYY and YYYYMM.  These shortened forms are not stored.
 */
public class DateHandler implements FieldHandler
{

  public void handle( org.apache.lucene.document.Document luceneDocument, Document document )
  {
    for ( String date : document.getAll( "date" ) )
      {
        // Store, but do not index, the full date.
        luceneDocument.add( new Field( "date", date, Field.Store.YES, Field.Index.NO  ) );

        // Index, but do not store, the year and the year+month.  These are what can be searched.
        if ( date.length() >= 6 )
          {
            luceneDocument.add( new Field( "date", date.substring( 0, 6 ), Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
          }
        if ( date.length() >= 4 )
          {
            luceneDocument.add( new Field( "date", date.substring( 0, 4 ), Field.Store.NO,  Field.Index.NOT_ANALYZED_NO_NORMS ) );
          }
      }
  }

}
