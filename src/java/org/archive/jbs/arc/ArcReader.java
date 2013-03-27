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

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.InputStream;

import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import org.archive.io.arc.ARCConstants;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;

import org.apache.commons.httpclient.Header;

/**
 * Convenience wrapper around the (W)ARC readers which allows for
 * simple iteration through an (W)ARC file, returning a series of
 * ArchiveRecordProxy objects.
 *
 * This is not a general purpose (W)ARC reading class.  It is tailored
 * to the needs of jbs.Parse.
 */
public class ArcReader implements Iterable<ArchiveRecordProxy>
{
  private ArchiveReader reader;

  // NOTE: See the setSizeLimit() method for details.
  private int sizeLimit = Integer.MAX_VALUE - 1024;

  /**
   * Construct an <code>ArchiveReader</code> with the
   * given path and <code>InputStream</code>.  The path
   * is used to indicate ARC vs. WARC.
   */
  public ArcReader( String path, InputStream is )
    throws IOException
  {
    this.reader = ArchiveReaderFactory.get( path, is, true );
    this.reader.setDigest( true );

    // If we are reading arc files, then we have to explictly enable
    // the parsing of the HTTP headers.  If we don't, then the call to
    // arc.skipHttpHeader() in the ArchiveRecordProxy will explode.
    //
    // BTW, we just try and cast it rather than using 'instanceof'
    // because we don't know which subtype of ARCReader it will
    // actually be.
    try
      {
        ((ARCReader)this.reader).setParseHttpHeaders(true);
      }
    catch ( ClassCastException cce )
      {
        // Eat it.
      }
  }

  /**
   * Construct an <code>ArcReader<code> wrapping an
   * <code>ArchiveReader</code> instance.
   *
   * @param reader the ArchiveReader instance to wrap
   */
  public ArcReader( ArchiveReader reader )
  {
    this.reader = reader;
  }

  /**
   * Limit the amount of bytes read from the archive record to prevent
   * exceeding the available heap size.  By default, the limit is 2GB,
   * which is the maxiumum size allowed.  The limit is 2GB because we
   * load the content into a byte[].
   *
   * Giving a negative value means the same thing as "the maximum
   * allowed value".
   *
   * FIXME: There seems to be a bug in the OpenJDK 7.0 (7.0_02-b13 at
   * least) where computing a SHA-1 digest over a byte buffer close to
   * Integer.MAX_VALUE in size triggers a core-dump in the JVM.
   * Experiments show that (Integer.MAX_VALUE-10) is the edge where
   * core dumps happen..anything larger than that value.
   *
   * So, even though MAX_VALUE-11 seems safe, we'll back it off a full
   * 1024 bytes, just in case.
   */
  public void setSizeLimit( int sizeLimit )
  {
    if ( sizeLimit < 0 || sizeLimit > (Integer.MAX_VALUE - 1024) )
      {
        this.sizeLimit = Integer.MAX_VALUE - 1024;
      }
    else
      {
        this.sizeLimit = sizeLimit;
      }
  }

  public int getSizeLimit( )
  {
    return this.sizeLimit;
  }

  /**
   * Returns an iterator over <code>ArchiveRecordProxy</code> objects,
   * which wrap the <code>WARCRecord</code>/<code>ARCRecord</code>
   * objects from the inner <code>ArchiveReader</code>.
   *
   * @return an iterator
   */
  public Iterator<ArchiveRecordProxy> iterator( )
  {
    return new ArchiveRecordProxyIterator( );
  }

  /**
   * Iterator over ArchiveRecordProxy objects.
   */
  private class ArchiveRecordProxyIterator implements Iterator<ArchiveRecordProxy>
  {
    private Iterator<ArchiveRecord> i;

    /**
     * Construct a <code>ArchiveRecordProxyIterator</code>, skipping the header
     * record if the wrapped reader is an <code>ARCReader</code>.
     */
    public ArchiveRecordProxyIterator( )
    {
      this.i = ArcReader.this.reader.iterator( );
    }

    /**
     * Returns <code>true</code> if the iteration has more elements.
     * Will return <code>true</code> even if the value returned by the
     * next call to <code>next()</code> returns <code>null</code>.
     *
     * @return <code>true</code> if the iterator has more elements.
     */
    public boolean hasNext( )
    {
      return this.i.hasNext( );
    }
    
    /**
     * Returns the next element in the iteration. Calling this method
     * repeatedly until the <code>hasNext()</code> method returns
     * <code>false</code> will return each element in the underlying
     * collection exactly once.
     * 
     * @return the next element in the iteration, which can be <code>null</code>
     */
    public ArchiveRecordProxy next( )
    {
      try
        {
          ArchiveRecord record = this.i.next( );
          
          if ( record instanceof ARCRecord )
            {
              ArchiveRecordProxy proxy = new ArchiveRecordProxy( (ARCRecord) record, sizeLimit );

              return proxy;
            }
          
          if ( record instanceof WARCRecord )
            {
              ArchiveRecordProxy proxy = new ArchiveRecordProxy( (WARCRecord) record, sizeLimit );

              return proxy;
            }

          // If we get here then the record we read in was neither an
          // ARC or WARC record.  What is a good exception to throw?
          throw new RuntimeException( "Record neither ARC nor WARC: " + record.getClass( ) );
        }
      catch ( IOException ioe )
        {
          throw new RuntimeException( ioe );
        }
    }

    /**
     * Unsupported optional operation.
     *
     * @throw UnsupportedOperationException
     */
    public void remove( )
    {
      throw new UnsupportedOperationException( );
    }

  }

  /**
   * Simple test/debug driver to read an archive file and print out
   * the header for each record.
   */
  public static void main( String args[] ) throws Exception
  {
    if ( args.length != 1 )
      {
        System.out.println( "ArcReader <(w)arc file>" );
        System.exit( 1 );
      }

    String arcName = args[0];

    ArchiveReader r = ArchiveReaderFactory.get( arcName );
    r.setDigest( true );

    ArcReader reader = new ArcReader( r );

    for ( ArchiveRecordProxy rec : reader )
      {
        if ( rec != null ) 
          {
            System.out.print( rec.getWARCRecordType()  + " " );
            System.out.print( rec.getWARCContentType() + " " );
            System.out.print( rec.getUrl()    + " " );
            System.out.print( rec.getDigest() + " " );
            System.out.print( rec.getDate()   + " " );
            System.out.print( rec.getLength() + " " );
            System.out.print( rec.getHttpStatusCode() );
            System.out.print( rec.getHttpResponseBody() != null ? rec.getHttpResponseBody().length : 0 );
            System.out.println( );
           }
      }
  }

}
