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

package org.archive.jbs.util;

import java.io.*;
import java.nio.*;
import java.util.*;

import org.apache.hadoop.io.Text;

/**
 * Read characters from a Hadoop Text object.
 */
public class TextReader extends Reader
{
  private ByteBuffer buf;

  public TextReader( Text text )
  {
    // Fill wrap the byte[] from the Text object.
    this.buf = ByteBuffer.wrap( text.getBytes(), 0, text.getLength() );
    this.buf.rewind();
  }

  /**
   *
   */
  @Override
  public void close() 
    throws IOException
  {
    // No-op
  }

  /**
   *
   */
  @Override
  public void mark( int readAheadLimit )
    throws IOException
  {
    throw new IOException( "mark not supported" );
  }

  /**
   *
   */
  @Override
  public boolean markSupported( ) 
  {
    return false;
  }

  /**
   *
   */
  @Override
  public int read( char[] cbuf, int offset, int len )
    throws IOException
  {
    if ( ! this.buf.hasRemaining() )
      {
        return -1;
      }

    int i = 0;
    for ( ; i < len && this.buf.hasRemaining() ; i++ )
      {
        int c = Text.bytesToCodePoint( this.buf );

        // FIXME: Handle surrogate pairs.
        cbuf[offset+i] = (char) c;
      }

    return i; 
  }

  /**
   *
   */
  @Override
  public void reset() 
    throws IOException
  {
    throw new IOException( "reset not supported" );
  }

}
