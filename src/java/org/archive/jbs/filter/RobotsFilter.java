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

package org.archive.jbs.filter;

import java.io.*;
import java.net.*;

import org.archive.jbs.Document;

/**
 * Simple DocumentFilter that filters out robots and favicon URLs.
 */
public class RobotsFilter implements DocumentFilter
{

  public boolean isAllowed( Document document )
  {
    String url  = document.get( "url" );
    
    try
      {
        URI uri = new URI( url );
        
        String path = uri.getPath();

        // If no path, then trivially *not* robots nor favicon.
        if ( path == null ) return true;

        path = path.trim();

        if ( "/favicon.ico".equals( path ) ||
             "/robots.txt" .equals( path ) )
          {
            return false;
          }
      } catch ( URISyntaxException e ) { }
    
    return true;
  }

}
