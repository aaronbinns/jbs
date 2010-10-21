
import java.io.*;
import java.util.*;

public class TypeFilter implements DocumentFilter
{
  public static final String[] DEFAULT_ALLOWED = 
    {
      "text/html", 
      "text/plain",
      "application/pdf", 
      // MS Office document types
      "application/msword", 
      "application/vnd.ms-powerpoint", 
      // OpenOffice document types
      "application/vnd.oasis.opendocument.text",
      "application/vnd.oasis.opendocument.presentation",
      "application/vnd.oasis.opendocument.spreadsheet",
    };

  private Set<String>    allowed;
  private TypeNormalizer normalizer;

  public TypeFilter( )
  {
    
  }

  public TypeFilter( Set<String> allowed, TypeNormalizer normalizer )
  {
    this.allowed    = allowed;
    this.normalizer = normalizer;
  }

  public void setTypeNormalizer( TypeNormalizer normalizer )
  {
    this.normalizer = normalizer;
  }

  public void setAllowed( Set<String> allowed )
  {
    this.allowed = allowed;
  }

  public Set<String> getAllowed( )
  {
    return this.allowed;
  }
  
  public boolean isAllowed( DocumentProperties properties )
  {
    String type = properties.get( "type" );

    // If no explicit list of allowed types, allow them all.
    if ( this.allowed == null || this.allowed.size( ) == 0 )
      {
        return true;
      }

    // De-alias it.
    type = this.normalizer.normalize( properties );

    return allowed.contains( type );
  }
  
  public static Set<String> parse( String s )
  {
    Set<String> types = new HashSet<String>( );

    for ( String type : s.split( "\\s+" ) )
      {
        if ( type.length() < 1 ) continue ;

        types.add( type );
      }

    return types;
  }

  public static Set<String> getDefaultAllowed( )
  {
    Set<String> defaults = new HashSet<String>( );
    
    for ( String allowed : DEFAULT_ALLOWED )
      {
        defaults.add( allowed );
      }

    return defaults;
  }

}
