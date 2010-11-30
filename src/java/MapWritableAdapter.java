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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * Handy utility class to wrap a MapWritable so it can be used with
 * DocumentFilters and stuff.
 */
public class MapWritableAdapter implements DocumentProperties
{
  MapWritable map;
  Text textKey;
  
  public MapWritableAdapter( MapWritable map )
  {
    this.map = map;
    this.textKey = new Text();
  }
  
  public String get( String key )
  {
    this.textKey.set( key );
    
    Text value = (Text) this.map.get( this.textKey );
    
    if ( value == null ) return "";
    
    return value.toString().trim();
  }
  
}
