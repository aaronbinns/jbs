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

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;

import org.archive.jbs.Document;
import org.archive.jbs.*;
import org.archive.jbs.filter.*;

/**
 * The LuceneDocumentWriter converts a Document into a Lucene Document
 * then adds it to the given index.
 *
 * Most of the interesting work is done by the DocumentFilters and
 * FieldHandlers.  The filters determine whether or not the document
 * is allowed and the various handlers convert the Document properties
 * into Lucene Fields.
 */
public class LuceneDocumentWriter extends DocumentWriterBase
{
  private IndexWriter indexer;
  private Analyzer    analyzer;

  private Map<String,FieldHandler> handlers;
  
  public LuceneDocumentWriter( IndexWriter indexer )
  {
    this.indexer  = indexer;
    this.analyzer = indexer.getAnalyzer();
  }

  public LuceneDocumentWriter( IndexWriter indexer, Analyzer analyzer )
  {
    this.indexer  = indexer;
    this.analyzer = analyzer;
  }

  public void setHandlers( Map<String,FieldHandler> handlers )
  {
    this.handlers = handlers;
  }

  public void add( String key, Document document )
    throws IOException
  {
    for ( DocumentFilter filter : filters.values() )
      {
        if ( ! filter.isAllowed( document ) )
          {
            return ;
          }
      }

    org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();

    for ( FieldHandler handler : handlers.values() )
      {
        handler.handle( doc, document );
      }

    indexer.addDocument( doc, analyzer );
  }
 
}
