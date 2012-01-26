/*
 * Copyright (C) 2008 Internet Archive.
 * 
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
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCRecord;

import org.apache.commons.httpclient.Header;

/**
 *
 */
public class ArcReader implements Iterable<ArchiveRecordProxy>
{
  private ArchiveReader reader;
  private long sizeLimit = -1;

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

  public void setSizeLimit( long sizeLimit )
  {
    this.sizeLimit = sizeLimit;
  }

  public long getSizeLimie( )
  {
    return this.sizeLimit;
  }

  /**
   * Returns an iterator over <code>ARCRecord</code>s in the wrapped
   * <code>ArchiveReader</code>, converting <code>WARCRecords</code>
   * to <code>ARCRecords</code> on-the-fly.
   *
   * @return an interator
   */
  public Iterator<ArchiveRecordProxy> iterator( )
  {
    return new ArchiveRecordProxyIterator( );
  }

  /**
   * 
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
      
      if ( ArcReader.this.reader instanceof ARCReader )
        {
          // Skip the first record, which is a "filedesc://"
          // record describing the ARC file.
          if ( this.i.hasNext( ) ) this.i.next( );
        }
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

          // If we get here then the record we reaad in was neither an ARC
          // or WARC record.  What is a good exception to throw?
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
            System.out.print( rec.getHttpResponseBody().length );
            System.out.println( );
           }
      }
  }

}