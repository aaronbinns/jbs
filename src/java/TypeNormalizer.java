
import java.io.*;
import java.util.*;


public class TypeNormalizer
{
  // Maps alias->canonical
  public static final String[][] DEFAULT_ALIASES =
    {
      // PDF aliases
      { "application/x-pdf", "application/pdf" },
      // HTML aliases.
      { "application/xhtml+xml", "text/html" },
      // MS Word aliases.
      { "application/vnd.ms-word", "application/msword" },
      { "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "application/msword" },
      // PowerPoint aliases.
      {"application/mspowerpoint",     "application/vnd.ms-powerpoint" },
      {"application/ms-powerpoint",    "application/vnd.ms-powerpoint" },
      {"application/mspowerpnt",       "application/vnd.ms-powerpoint" },
      {"application/vnd-mspowerpoint", "application/vnd.ms-powerpoint" },
      {"application/powerpoint",       "application/vnd.ms-powerpoint" },
      {"application/x-powerpoint",     "application/vnd.ms-powerpoint" },
      {"application/vnd.openxmlformats-officedocument.presentationml.presentation", "application/vnd.ms-powerpoint" },
    };

  private Map<String,String> aliases;

  public static Map<String,String> getDefaultAliases( )
  {
    Map<String,String> defaults = new HashMap<String,String>( );

    for ( String[] alias : DEFAULT_ALIASES )
      {
        defaults.put( alias[0], alias[1] );
      }

    return defaults;
  }

  public static Map<String,String> parseAliases( String s )
  {
    Map<String,String> aliases = new HashMap<String,String>( );
    
    for ( String line : s.split( "\\s+" ) )
      {
        if ( line.length() < 1 ) continue ;

        String[] tokens = line.split( "[:,]" );
        
        if ( tokens.length < 2 ) continue ;
        
        String type = tokens[0];
        
        if ( type.length() < 1 ) continue ;

        for ( int i = 1; i < tokens.length ; i++ )
          {
            aliases.put( tokens[i], type );
          }
      }

    return aliases;
  }

  public void setAliases( Map<String,String> aliases )
  {
    this.aliases = aliases;
  }
  
  public Map<String,String> getAliases( )
  {
    return this.aliases;
  }
  
  public String normalize( DocumentProperties properties )
  {
    String type = properties.get( "type" );

    // Chop off anything after a ';' character.  This is
    // for stuff like: "text/html; charset=utf-8"
    int p = type.indexOf( ';' );
    if ( p >= 0 ) type = type.substring( 0, p ).trim();

    if ( this.aliases != null && this.aliases.containsKey( type ) )
      {
        type = this.aliases.get( type );
      }

    return type;
  }

}
