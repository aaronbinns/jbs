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

import org.elasticsearch.node.Node;
import org.elasticsearch.client.Client;
import org.elasticsearch.action.index.IndexResponse;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;


/**
 * 
 */
public class ElasticSearchDocumentWriter extends DocumentWriterBase
{
  private Node node;
  private Client client;

  public ElasticSearchDocumentWriter( )
    throws IOException
  {
    this.node   = nodeBuilder().node();
    this.client = node.client();
  }

  public void setTypeNormalizer( TypeNormalizer typeNormalizer )
  {
    this.typeNormalizer = typeNormalizer;
  }

  public void add( String key, DocumentProperties properties )
    throws IOException
  {
    for ( DocumentFilter filter : filters.values() )
      {
        if ( ! filter.isAllowed( properties ) )
          {
            return ;
          }
      }
    
    // Create Solr XML document, add the fields, then add the document
    // to the index.
    String collection = properties.get( "collection" );
    collection = "".equals( collection ) ? "default" : collection;

    String date = properties.get( "date" );
    date = date.length() != "yyyymmddhhmmss".length() ? "1971-11-13T00:00:00Z" :
      date.substring(0,4)  + "-" + date.substring(4,6)   + "-" + date.substring(6,8)   + "T" +
      date.substring(8,10) + ":" + date.substring(10,12) + ":" + date.substring(12,14) + "Z" ;

    String type = this.typeNormalizer.normalize( properties );

    IndexResponse response = client.prepareIndex( collection, "web", Long.toHexString( FPGenerator.std64.fp( key ) ) )
        .setSource( jsonBuilder()
                    .startObject()
                      .field( "url",     properties.get( "url"    ) )
                      .field( "title",   properties.get( "title"  ) )
                      .field( "length",  properties.get( "length" ) )
                      .field( "content", properties.get( "content_parsed" ) )
                      .field( "date",    date )
                      .field( "type",    type )
                    .endObject()
                  )
        .execute()
        .actionGet();
  }

  public void close( )
    throws IOException
  {
    try
      {
        this.node.close();
      }
    catch ( Exception e )
      {
        throw new IOException( e );
      }
  }
}
