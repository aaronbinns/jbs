
import org.apache.lucene.search.DefaultSimilarity;

/** 
 * Similarity implementatation appropriate for web searching.
 * Intitially, taken from NutchSimilarity, then tweaked.
 */ 
public class WebSimilarity extends DefaultSimilarity  
{
  private static final int MIN_CONTENT_LENGTH = 1000;
  
  /** Normalize field by length.  Called at index time. */
  public float lengthNorm( String fieldName, int numTokens )
  {
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
