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

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class SolrOutputFormat extends FileOutputFormat<Text, MapWritable>
{
  public RecordWriter<Text, MapWritable> getRecordWriter( final FileSystem fs,
                                                          final JobConf job,
                                                          final String name,
                                                          final Progressable progress )
    throws IOException
  {
    String serverUrl = job.get( "indexer.solr.url", "http://localhost:8983/solr" );

    SolrDocumentWriter solrDocWriter = new SolrDocumentWriter( new URL( serverUrl ) );

    TypeNormalizer normalizer = new TypeNormalizer( );
    Map<String,String> aliases = normalizer.parseAliases( job.get( "indexer.typeNormalizer.aliases", "" ) );

    if ( job.getBoolean( "indexer.typeNormalizer.useDefaults", true ) )
      {
        Map<String,String> defaults = normalizer.getDefaultAliases( );
        defaults.putAll( aliases );

        aliases = defaults;
      }
    normalizer.setAliases( aliases );

    TypeFilter typeFilter = new TypeFilter( );
    Set<String> allowedTypes = typeFilter.parse( job.get( "indexer.typeFilter.allowed", "" ) );

    if ( job.getBoolean( "indexer.typeFilter.useDefaults", true ) )
      {
        Set<String> defaults = typeFilter.getDefaultAllowed( );
        defaults.addAll( allowedTypes );

        allowedTypes = defaults;
      }
    typeFilter.setAllowed( allowedTypes );
    typeFilter.setTypeNormalizer( normalizer );

    solrDocWriter.setFilter( "reqFields", new RequiredFieldsFilter( ) );
    solrDocWriter.setFilter( "type",      typeFilter );
    solrDocWriter.setFilter( "robots",    new RobotsFilter( ) );    

    solrDocWriter.setTypeNormalizer( normalizer );

    return new SolrRecordWriter( solrDocWriter );
  }
  
  public class SolrRecordWriter implements RecordWriter<Text, MapWritable>
  {
    SolrDocumentWriter docWriter;
    
    public SolrRecordWriter( SolrDocumentWriter docWriter )
    {
      this.docWriter = docWriter;
    }

    public void write( Text key, MapWritable properties )
      throws IOException
    {
      this.docWriter.add( key, properties );
    }
    
    public void close( Reporter reporter )
    {
      // Send a solr message to commit.  Anything else?
      try
        {
          docWriter.commit();
        }
      catch ( IOException ioe )
        {
          // TODO: Log it.
        }
    }
  }
}
