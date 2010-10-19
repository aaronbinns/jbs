
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