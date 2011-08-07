/**
 * This code is derived from the class NutchSimilarity distributed
 * with Nutch 1.1.  The original license is as follows:
 *
 *   Licensed to the Apache Software Foundation (ASF) under one or
 *   more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information regarding
 *   copyright ownership.  The ASF licenses this file to You under the
 *   Apache License, Version 2.0 (the "License"); you may not use this
 *   file except in compliance with the License.  You may obtain a
 *   copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS
 *   IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied.  See the License for the specific language
 *   governing permissions and limitations under the License.
 */

package org.archive.jbs.lucene;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.DefaultSimilarity;

/** 
 * Similarity implementatation appropriate for web searching.
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
    else if ("content".equals(fieldName))
      {    
        // Content: penalize short, by treating short as longer

        // TODO: Is creating a new FieldInvertState object good, or
        //       should we modify the existing one?
        return super.computeNorm( fieldName, new FieldInvertState( state.getPosition(),
                                                                   Math.max(numTokens, MIN_CONTENT_LENGTH),
                                                                   state.getNumOverlap(),
                                                                   state.getOffset(),
                                                                   state.getBoost() 
                                                                 ) );
      }
    else
      {
        // use default
        return super.computeNorm(fieldName, state);
      }
  }
  
  public float coord(int overlap, int maxOverlap)
  {
    return 1.0f;
  }

}
