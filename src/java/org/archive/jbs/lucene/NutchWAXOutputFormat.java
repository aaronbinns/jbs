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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.lucene.store.*;

import org.archive.jbs.Document;
import org.archive.jbs.*;
import org.archive.jbs.util.*;
import org.archive.jbs.filter.*;

/**
 *
 */
public class NutchWAXOutputFormat extends LuceneOutputFormat
{
    protected LuceneDocumentWriter buildDocumentWriter( JobConf job, IndexWriter indexer )
    throws IOException
  {
    // This configuration propery must be set to an actual file,
    // otherwise the Nutch CommonGrams class explodes (which is
    // invoked by the NutchDocumentAnalyzer).  This empty
    // "common-terms.utf8" file is bundled into the JBs .jar file.
    job.set( "analysis.common.terms.file", "common-terms.utf8" );

    Analyzer analyzer = new org.apache.nutch.analysis.NutchDocumentAnalyzer( job );

    LuceneDocumentWriter writer = new LuceneDocumentWriter( indexer, analyzer );

    TypeNormalizer normalizer = buildTypeNormalizer( job );
    TypeFilter     typeFilter = buildTypeFilter( job, normalizer );

    writer.setFilter( "reqFields", new RequiredFieldsFilter( ) );
    writer.setFilter( "type",      typeFilter );
    writer.setFilter( "robots",    new RobotsFilter( ) );
    writer.setFilter( "http",      new HTTPStatusCodeFilter( job.get( "jbs.httpStatusCodeFilter" ) ) );

    int textMaxLength = job.getInt( "jbs.lucene.text.maxlength", TextHandler.MAX_LENGTH );

    Map<String,FieldHandler> handlers = new HashMap<String,FieldHandler>( );
    handlers.put( "url"        , new SimpleFieldHandler( "url",         Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "digest"     , new SimpleFieldHandler( "digest",      Field.Store.YES, Field.Index.NO       ) );
    handlers.put( "title"      , new SimpleFieldHandler( "title",       Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "keywords"   , new SimpleFieldHandler( "keywords",    Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "description", new SimpleFieldHandler( "description", Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "length"     , new SimpleFieldHandler( "length",      Field.Store.YES, Field.Index.NO ) );
    handlers.put( "code"       , new SimpleFieldHandler( "code",        Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
    handlers.put( "content"    , new TextHandler( "content", textMaxLength ) );
    handlers.put( "boiled"     , new TextHandler( "boiled" , textMaxLength ) );
    handlers.put( "date"       , new DateHandler( ) );
    handlers.put( "site"       , new NutchWAXSiteHandler( ) );
    handlers.put( "type"       , new TypeHandler( normalizer ) );  
    handlers.put( "boost"      , new BoostHandler( ) );

    String collection = job.get( "jbs.lucene.collection", null );
    if ( collection == null )
      handlers.put( "collection", new SimpleFieldHandler( "collection", Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
    else
      handlers.put( "collection", new FixedValueFieldHandler( "collection", collection, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );

    writer.setHandlers( handlers );
    
    return writer;
  }


}
