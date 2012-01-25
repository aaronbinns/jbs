/*
 * This file is part of the archive-access tools project
 * (http://sourceforge.net/projects/archive-access).
 * 
 * The archive-access tools are free software; you can redistribute them and/or
 * modify them under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or any
 * later version.
 * 
 * The archive-access tools are distributed in the hope that they will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser Public License along with
 * the archive-access tools; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
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
   * 
   */
  public ArchiveRecordProxy( ARCRecord arc, long sizeLimit )
    throws IOException
  {
    ARCRecordMetaData header = (ARCRecordMetaData) arc.getHeader( );

    // FIXME: Can arcs contain dns records?  Or other non-HTTP response types?
    this.warcRecordType  = WARCConstants.RESPONSE;
    this.warcContentType = WARCConstants.HTTP_RESPONSE_MIMETYPE;

    this.url    = header.getUrl();
    // No digest until after the record is fully read.
    this.date   = header.getDate();
    this.length = header.getLength();
    this.code   = header.getStatusCode();

    this.body = readBytes( arc, this.length, sizeLimit );
    // Must "close" the record to complete the digest computation.
    arc.close();
    this.digest = "sha1:" + arc.getDigestStr();
  }

  /**
   * 
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
        // of types remaining in the WARC record.q
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
    while ( (pos += record.read( bytes, pos, (bytes.length - pos) )) < bytes.length )
      ;
    
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