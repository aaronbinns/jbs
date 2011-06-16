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

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.DefaultSimilarity;

/** 
 * Lucene Similarity implementatation appropriate for web searching.
 * Intitially, taken from NutchSimilarity, then tweaked.
 */ 
public class WebSimilarity extends DefaultSimilarity  
{
  private static final int MIN_CONTENT_LENGTH = 1000;
  
  /** Normalize field by length.  Called at index time. */
  public float computeNorm( String fieldName, FieldInvertState state )
  {
    int numTokens = state.getLength( );

    if ("url".equals(fieldName))
      {
        // URL: prefer short by using linear normalization
        return 1.0f / numTokens;
        
      }
    else if ("anchor".equals(fieldName))
      { 
        // Anchor: prefer more
        return (float)(1.0/Math.log(Math.E+numTokens)); // use log
        
      }
    else if ("content".equals(fieldName))
      {    
        // Content: penalize short, by treating short as longer
        return super.lengthNorm( fieldName, Math.max(numTokens, MIN_CONTENT_LENGTH) );
      }
    else
      {
        // use default
        return super.lengthNorm(fieldName, numTokens);
      }
  }
  
  public float coord(int overlap, int maxOverlap)
  {
    return 1.0f;
  }

}
