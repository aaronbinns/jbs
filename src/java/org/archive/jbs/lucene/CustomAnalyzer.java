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
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.util.Version;


/**
 * Simple custom analyzer that combines a bunch of common filters.
 * This would be nice to have in an external config file, but for
 * now we just have this little hard-coded class.
 */
public class CustomAnalyzer extends Analyzer
{
  boolean omitNonAlpha = true;
  Set<?> stopWords;

  public CustomAnalyzer( )
  {

  }

  public CustomAnalyzer( boolean omitNonAlpha, Set<?> stopWords )
  {
    this.omitNonAlpha = omitNonAlpha;
    this.stopWords    = stopWords;
  }
  
  public void setStopWords( Set<?> stopWords )
  {
    this.stopWords = stopWords;
  }

  public Set<?> getStopWords( )
  {
    return this.stopWords;
  }

  public void setOmitNonAlpha( boolean omitNonAlpha )
  {
    this.omitNonAlpha = omitNonAlpha;
  }

  public boolean getOmitNonAlpha( )
  {
    return this.omitNonAlpha;
  }

  public TokenStream tokenStream( String fieldName, Reader reader )
  {
    TokenStream stream = new StandardTokenizer( Version.LUCENE_30, reader );
    stream = new StandardFilter( stream );
    stream = new LowerCaseFilter( stream );
    stream = new ASCIIFoldingFilter( stream );
    if ( this.omitNonAlpha )
      {
        stream = new NonAlphaFilter( stream );
      }
    if ( this.stopWords != null && this.stopWords.size( ) > 0 )
      {
        stream = new StopFilter( true, stream, this.stopWords );
      }

    return stream;
  }

}
