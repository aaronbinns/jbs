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
import java.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.Version;


/**
 * Simple filter that only passes tokens with at least one alphabetic
 * character.  The goal is to exclude tokens that are all numbers or
 * just numbers and punctuation, i.e. stuff like "7009-2_#".
 */
class NonAlphaFilter extends TokenFilter
{
  private TermAttribute termAtt;
  private PositionIncrementAttribute posIncrAtt;

  public NonAlphaFilter( TokenStream in )
  {
    super( in );
    termAtt    = addAttribute(TermAttribute.class);
    posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  }

  public boolean incrementToken( )
    throws IOException
  {
    // return the first non-stop word found
    int skippedPositions = 0;

    while ( input.incrementToken( ) )
      {
        if ( hasAlpha( termAtt.termBuffer( ), 0, termAtt.termLength( ) ) )
          {
            posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
            
            return true;
          }
        skippedPositions += posIncrAtt.getPositionIncrement();
    }

    // reached EOS -- return false
    return false;
  }

  private boolean hasAlpha( char[] buf, int offset, int length )
  {
    for ( int i = offset ; i < length ; i++ )
      {
        char c = buf[i];

        if ( Character.isLetter( c ) ) return true;
      }

    return false;
  }
}