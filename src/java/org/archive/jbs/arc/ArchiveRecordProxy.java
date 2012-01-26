/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.archive.jbs.arc;

import java.io.*;
import java.util.*;

import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCConstants;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.HttpParser;

/**
 * Proxy object for ARC and WARC records.  It reads the headers from
 * either one and extracts out just the metadata we need for parsing.
 * ARC records are mapped to the corresponding WARC record type.
 *
 * This is not a general purpose (W)ARC reading class.  It is tailored
 * to the needs of jbs.Parse.
 *
 * A sizeLimit is passed to the constructor to limit the number of
 * bytes that are read from the record body, to avoid blowing up the
 * heapspace.  Some (W)ARC records can have 100+GB record bodies --
 * for example video stream captures.
 *
 * Note that for HTTP response records, the 'length' field in the
 * proxy object is the length of the *document*, that is the HTTP
 * response *body*.  The length stored in the (W)ARC header includes
 * the HTTP status line and headers, which we don't want.  What we
 * want is the *document* length.
 *
 * For other record types, the length as given in the (W)ARC record
 * header information.
 */
public class ArchiveRecordProxy
{
  private String warcRecordType;
  private String warcContentType;
  private String url;
  private String digest;
  private String date;
  private long   length;
  private String code;
  private byte[] body;

  /**
   * Construct an ARCRecord proxy.  Read at most sizeLimit
   * bytes from the record body.
   */
  public ArchiveRecordProxy( ARCRecord arc, long sizeLimit )
    throws IOException
  {
    ARCRecordMetaData header = (ARCRecordMetaData) arc.getHeader( );

    this.url    = header.getUrl();
    // No digest until after the record is fully read.
    this.date   = header.getDate();
    this.length = header.getLength();
    this.code   = header.getStatusCode();

    // Move the file position past the HTTP headers to the start of
    // the HTTP response body.  We can't use the
    // ARCRecord.skipHttpHeader() method because it only works if the
    // ARCRecord is constructed in a particular manner, and it looks
    // like it's broken in this case.
    if ( url.startsWith( "http" ) )
      {
        this.warcRecordType  = WARCConstants.RESPONSE;
        this.warcContentType = WARCConstants.HTTP_RESPONSE_MIMETYPE;

        arc.skipHttpHeader();
        this.length = arc.available();
        this.body = readBytes( arc, this.length, sizeLimit );
      }
    else if ( url.startsWith( "filedesc" ) )
      {
        this.warcRecordType  = WARCConstants.WARCINFO;
        this.warcContentType = "application/arcinfo";
      }
    else
      {
        throw new IOException( "Unknown ARC record type: " + url );
      }

    // Must "close" the record to complete the digest computation.
    arc.close();
    this.digest = "sha1:" + arc.getDigestStr();
  }

  /**
   * Construct an WARCRecord proxy.  Read at most sizeLimit
   * bytes from the record body.
   */
  public ArchiveRecordProxy( WARCRecord warc, long sizeLimit )
    throws IOException
  {
    ArchiveRecordHeader header = warc.getHeader( );

    this.warcRecordType  = (String) header.getHeaderValue( WARCConstants.HEADER_KEY_TYPE );
    this.warcContentType = (String) header.getHeaderValue( WARCConstants.CONTENT_TYPE    );

    this.url    = header.getUrl();
    this.digest = (String) header.getHeaderValue( WARCConstants.HEADER_KEY_PAYLOAD_DIGEST );

    // Convert to familiar YYYYMMDDHHMMSS format
    String warcDate = (String) header.getHeaderValue( WARCConstants.HEADER_KEY_DATE );
    this.date = new StringBuilder( )
      .append( warcDate,  0, 4  )
      .append( warcDate,  5, 7  )
      .append( warcDate,  8, 10 )
      .append( warcDate, 11, 13 )
      .append( warcDate, 14, 16 )
      .append( warcDate, 17, 19 )
      .toString();

    // Check if HTTP (not dns or the like) and then read the HTTP
    // headers so that the file position is at the response body
    if ( WARCConstants.HTTP_RESPONSE_MIMETYPE.equals( this.warcContentType ) )
      {
        StatusLine statusLine = new StatusLine( HttpParser.readLine( warc, "utf-8" ) );
        this.code = Integer.toString( statusLine.getStatusCode() );
        
        // Slurp up the rest of the HTTP headers, discarding them.
        Header[] headers = HttpParser.parseHeaders( warc, "utf-8" );

        // The length of the HTTP response body is equal to the number
        // of types remaining in the WARC record.
        this.length = warc.available();

        this.body = readBytes( warc, this.length, sizeLimit );
      }
  }

  /**
   * Read the bytes of the record into a buffer and return the buffer.
   * Give a size limit to the buffer to prevent from exploding memory,
   * but still read all the bytes from the stream even if the buffer
   * is full.  This was the file position will be advanced to the end
   * of the record.
   */
  private byte[] readBytes( ArchiveRecord record, long contentLength, long sizeLimit )
    throws IOException
  {
    // Ensure the record does strict reading.
    record.setStrict( true );

    if ( sizeLimit < 0 )
      {
        sizeLimit = contentLength;
      }
    else
      {
        sizeLimit = Math.min( sizeLimit, contentLength );
      }

    byte[] bytes = new byte[(int) sizeLimit];

    if ( sizeLimit == 0 )
      {
        return bytes;
      }
    
    // NOTE: Do not use read(byte[]) because ArchiveRecord does NOT over-ride
    //       the implementation inherited from InputStream.  And since it does
    //       not over-ride it, it won't do the digesting on it.  Must use either
    //       read(byte[],offset,length) or read().
    int pos = 0;
    int c   = 0;
    while ( ((c = record.read( bytes, pos, (bytes.length - pos) )) != -1) && pos < bytes.length )
      {
        pos += c;
      }
    
    // Now that the bytes[] buffer has been filled, read the remainder
    // of the record so that the digest is computed over the entire
    // content.
    byte[] buf = new byte[1024 * 1024];
    int count = 0;
    while ( record.available( ) > 0 )
      {
        count += record.read( buf, 0, Math.min( buf.length, record.available( ) ) );
      }
    
    // Sanity check.  The number of bytes read into our bytes[]
    // buffer, plus the count of extra stuff read after it should
    // equal the contentLength passed into this function.
    if ( pos + count != contentLength )
      {
        throw new IOException( "Incorrect number of bytes read from ArchiveRecord: expected=" + contentLength + " bytes.length=" + bytes.length + " pos=" + pos + " count=" + count );
      }
    
    return bytes;
  }

  public String getWARCRecordType()
  {
    return this.warcRecordType;
  }

  public String getWARCContentType()
  {
    return this.warcContentType;
  }

  public String getUrl()
  {
    return this.url;
  }

  public String getDigest()
  {
    return this.digest;
  }

  public String getDate()
  {
    return this.date;
  }

  public long getLength()
  {
    return this.length;
  }

  public String getHttpStatusCode()
  {
    return this.code;
  }

  public byte[] getHttpResponseBody()
  {
    return this.body;
  }
  
}