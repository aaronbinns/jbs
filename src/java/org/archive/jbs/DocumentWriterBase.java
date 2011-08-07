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

package org.archive.jbs;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.archive.jbs.filter.*;

/**
 * Sub-classes take the given MapWritable, create a document from it
 * and write it to an index.
 */
public abstract class DocumentWriterBase implements DocumentWriter
{
  protected Map<String,DocumentFilter> filters = new HashMap<String,DocumentFilter>( );
  protected TypeNormalizer typeNormalizer;

  public DocumentFilter getFilter( String name )
  {
    return this.filters.get( name );
  }

  public void setFilter( String name, DocumentFilter filter )
  {
    this.filters.put( name, filter );
  }

  public abstract void add( String key, Document document ) throws IOException;
}
