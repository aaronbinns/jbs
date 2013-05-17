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
import org.apache.commons.httpclient.HttpException;

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
 * Note that for HTTP response records, the 'length' field in this
 * proxy object is the length of the the HTTP response *body*.  The
 * length stored in the (W)ARC header includes the HTTP status line
 * and headers, which we don't want.
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
  public ArchiveRecordProxy( ARCRecord arc, int sizeLimit )
    throws IOException
  {
    ARCRecordMetaData header = (ARCRecordMetaData) arc.getHeader( );

    this.url    = header.getUrl();
    // No digest until after the record is fully read.
    this.date   = header.getDate();
    this.code   = header.getStatusCode();

    if ( url.startsWith( "http" ) )
      {
        this.warcRecordType  = WARCConstants.WARCRecordType.RESPONSE.toString();
        this.warcContentType = WARCConstants.HTTP_RESPONSE_MIMETYPE;

        // Move the file position past the HTTP headers to the start of
        // the HTTP response body.
        arc.skipHttpHeader();
        
        // The length of the HTTP response body is equal to the number
        // of bytes remaining in the arc record.
        // UNLESS the HTTP response body is empty, in which case the
        // arc.getPosition() is already one byte past the
        // header.getLength().
        this.length = header.getLength() - arc.getPosition();
        if ( this.length == -1 ) this.length = 0;

        this.body = readBytes( arc, this.length, sizeLimit );
      }
    else if ( url.startsWith( "filedesc" ) )
      {
        this.warcRecordType  = WARCConstants.WARCRecordType.WARCINFO.toString();
        this.warcContentType = "application/arcinfo";
      }
    else if ( url.startsWith( "dns:" ) )
      {
        this.warcRecordType  = WARCConstants.WARCRecordType.RESPONSE.toString();
        this.warcContentType = "text/dns";
      }
    else if ( url.startsWith( "ftp:" ) )
      {
        this.warcRecordType  = WARCConstants.WARCRecordType.RESOURCE.toString();

        // The mime-type in the ARC header tells us whether this is part of the 
        // control conversation or the actual downloaded file.
        if ( "text/plain".equals( header.getMimetype( ) ) )
          {
            this.warcContentType = "text/plain";
          }
        else
          {
            this.warcContentType = "application/octet-stream";
          }

        // HACK: We set a bogus HTTP status code 200 here because
        //       later in the indexing workflow, non-200 codes are
        //       filtered out so that we don't index 404 pages and
        //       such.
        this.code = "200";

        this.length = header.getLength() - arc.getPosition();
        if ( this.length == -1 ) this.length = 0;

        this.body = readBytes( arc, this.length, sizeLimit );
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
  public ArchiveRecordProxy( WARCRecord warc, int sizeLimit )
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
        // Sometimes an HTTP response will have whitespace (such as
        // blank lines) before the actual HTTP status line like:
        //   [blank]
        //   HTTP/1.0 200 OK
        // so we have to gobble them up.
        String line;
        while ( (line = HttpParser.readLine( warc, "utf-8" )) != null )
          {
            line = line.trim();

            // If an empty line, or an invalid HTTP-status line: skip it!
            if ( line.length() == 0 ) continue ;
            if ( ! line.startsWith( "HTTP" ) ) continue;

            try
              {
                // Now get on with parsing the status line.
                StatusLine statusLine = new StatusLine( line );
                this.code = Integer.toString( statusLine.getStatusCode() );
                break;
              }
            catch ( HttpException e )
              {
                // The line started with "HTTP", but was not a full,
                // valid HTTP-Status line.  Assume that we won't see
                // one, so break out of the loop.
                // But first, set the HTTP code to a value indicating
                // there was no valid HTTP code.
                this.code = "";
                break;
              }
          }
        
        // Skip over the HTTP headers, we just want the body of the HTTP response.
        skipHttpHeaders( warc );

        // The length of the HTTP response body is equal to the number
        // of bytes remaining in the WARC record.
        this.length = header.getLength() - warc.getPosition();

        this.body = readBytes( warc, this.length, sizeLimit );
      }
    else if ( WARCConstants.WARCRecordType.RESOURCE.toString().equals( this.warcRecordType ) 
              &&
              // We check for "ftp://" here because we don't want to waste the time to copy the
              // bytes for resource record types we don't care about.  If we want to pass along
              // all resource records, simply remove this check.
              this.url.startsWith( "ftp://" ) )
      {
        // HACK: We set a bogus HTTP status code 200 here because
        //       later in the indexing workflow, non-200 codes are
        //       filtered out so that we don't index 404 pages and
        //       such.
        this.code = "200";
        
        // The length of the FTP response body is equal to the number
        // of bytes remaining in the WARC record.
        this.length = header.getLength() - warc.getPosition();

        this.body = readBytes( warc, this.length, sizeLimit );
      }
             
  }

  /**
   * Skip over the HTTP headers.  We use a simple 3-state FSM to
   * search for the byte sequence: \n( |\r)*\n
   *
   * Rather than use the Apache HttpParser.read* methods, I
   * implemented this here to avoid any assumptions that class makes
   * about character encoding.
   *
   * And for sure, avoid HttpParser.parseHeaders() as it is strict
   * about the header format and will throw an exception if they are
   * malformed.  We don't care, we just want to skip them.
   */
  private void skipHttpHeaders( WARCRecord warc ) throws IOException
  {
    int ch;
    int state = 0;
    while ( (state != 2) && (ch = warc.read() ) >= 0 )
      {
        switch ( state )
          {
          case 0:
            if ( ch == '\n' ) state = 1;
            break;
            
          case 1:
            if ( ch == '\n' ) 
              state = 2;
            else if ( ch == '\r' || ch == ' ' )
              state = 1;
            else
              state = 0;
            break;
          }
      }
  }

  /**
   * Read the bytes of the record into a buffer and return the buffer.
   * Give a size limit to the buffer to prevent from exploding memory,
   * but still read all the bytes from the stream even if the buffer
   * is full.  This way, the file position will be advanced to the end
   * of the record.
   */
  private byte[] readBytes( ArchiveRecord record, long contentLength, int sizeLimit )
    throws IOException
  {
    // Ensure the record does strict reading.
    record.setStrict( true );

    sizeLimit = (int) Math.min( sizeLimit, contentLength );

    byte[] bytes = new byte[sizeLimit];

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
    long count = 0;
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
