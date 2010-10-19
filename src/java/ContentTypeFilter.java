
import java.io.*;
import java.util.*;

public class ContentTypeFilter
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

  private Set<String>        allowed;
  private Map<String,String> aliases;

  public ContentTypeFilter( )
  {
    
  }

  public ContentTypeFilter( Set<String> allowed, Map<String,String> aliases )
  {
    this.allowed = allowed;
    this.aliases = aliases;
  }

  public void setAllowed( Set<String> allowed )
  {
    this.allowed = allowed;
  }

  public Set<String> getAllowed( )
  {
    return this.allowed;
  }
  
  public void setAliases( Map<String,String> aliases )
  {
    this.aliases = aliases;
  }

  public Map<String,String> getAliases( )
  {
    return this.aliases;
  }

  public boolean isAllowed( String type )
  {
    // If no explicit list of allowed types, allow them all.
    if ( this.allowed == null || this.allowed.size( ) == 0 )
      {
        return true;
      }

    type = type.trim();

    // De-alias it.
    if ( this.aliases.containsKey( type ) )
      {
        type = this.aliases.get( type );
      }

    return allowed.contains( type );
  }
  
  public static Set<String> parseContentTypes( String s )
  {
    Set<String> types = new HashSet<String>( );

    for ( String type : s.split( "\\s+" ) )
      {
        if ( type.length() < 1 ) continue ;

        types.add( type );
      }

    return types;
  }

  public static Map<String,String> parseContentTypeAliases( String s )
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

  public static Set<String> getDefaultAllowed( )
  {
    Set<String> defaults = new HashSet<String>( );
    
    for ( String allowed : DEFAULT_ALLOWED )
      {
        defaults.add( allowed );
      }

    return defaults;
  }

  public static Map<String,String> getDefaultAliases( )
  {
    Map<String,String> defaults = new HashMap<String,String>( );

    for ( String[] alias : DEFAULT_ALIASES )
      {
        defaults.put( alias[0], alias[1] );
      }

    return defaults;
  }

}
