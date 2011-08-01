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
import java.util.concurrent.*;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.client.solrj.*;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

/**
 * 
 */
public class SolrDocumentWriter extends DocumentWriterBase
{
  private SolrServer server;
  private Queue<SolrInputDocument> docBuffer;
  private TypeNormalizer typeNormalizer;

  public SolrDocumentWriter( URL url, int docBufferSize )
    throws IOException
  {
    this.server    = new CommonsHttpSolrServer( url );
    this.docBuffer = new ArrayBlockingQueue<SolrInputDocument>( docBufferSize );
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
    SolrInputDocument doc = new SolrInputDocument();

    doc.addField( "id",  key );
    
    for ( String p : new String[] { "url", "title", "length", "collection", "boiled" } )
      {
        String value = properties.get( p );

        if ( value.length() > 0 ) doc.addField( p, properties.get( p ) );
      }

    doc.addField( "content", properties.get( "content_parsed" ) );

    // Solr requires the date to be in the form: 1995-12-31T23:59:59Z
    // See the Solr schema docs.
    String date = properties.get( "date" );
    if ( date.length() == "yyyymmddhhmmss".length() )
      {
        doc.addField( "date", date.substring(0,4)  + "-" + date.substring(4,6)   + "-" + date.substring(6,8)   + "T" + 
                              date.substring(8,10) + ":" + date.substring(10,12) + ":" + date.substring(12,14) + "Z" );
      }

    // Special handling for site and tld
    try
      {
        String url = properties.get( "url" );
        
        String site = (new URL( url)).getHost( );
        
        // Strip off any "www[0-9]*." header.
        site = site.toLowerCase().replaceFirst( "^www[0-9]*[.]", "" );
        
        // Special rule for Photobucket
        site = site.replaceAll( "^[a-z0-9]+.photobucket.com$", "photobucket.com" );
        
        doc.addField( "site", site );

        // Add the tld: com, org, net, uk, fr, etc.
        if ( site.length() > 0 
             && site.charAt(0) != '['                             // Not an IPv6 address
             && ! site.matches("(?:[0-9]{1,3}[.]){3}[0-9]{1,3}")  // Not an IPv4 address
             )
          {
            String[] s = site.replaceFirst("[.]$", "").split( "[.]" );

            String tld = s[s.length-1];

            doc.addField( "tld", tld );
          }
      }
    catch ( MalformedURLException mue )
      {
        // Rut-roh.
      }

    // Special handling for type
    String type = this.typeNormalizer.normalize( properties );
    
    doc.addField( "type", type );

    // Finally, add the document.
    try
      {
        if ( ! this.docBuffer.offer( doc ) )
          {
            // The buffer is full, send the buffered documents.
            this.server.add( this.docBuffer );

            // Clear the buffer and add the document.
            this.docBuffer.clear();
            this.docBuffer.offer( doc );
          }
      }
    catch ( SolrServerException sse )
      {
        throw new IOException( sse );
      }
  }

  public void commit( )
    throws IOException
  {
    try
      {
        // Send any documents still in the buffer
        this.server.add( this.docBuffer );

        // Commit the updates.
        this.server.commit();
      }
    catch ( SolrServerException sse )
      {
        throw new IOException( sse );
      }
  }
}
