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

/**
 * This class is inspired by the technique used in Nutch's
 * LuceneOutputFormat class.  However, rather than creating a Lucene
 * index and writing documents to it, we use the SolrJ API to send the
 * documents to a (remote) Solr server.
 *
 * We perform essentially the same type normalization and filtering,
 * robot filtering, etc. before forming the Solr document.
 */
public class ElasticSearchOutputFormat extends FileOutputFormat<Text, MapWritable>
{
  public RecordWriter<Text, MapWritable> getRecordWriter( final FileSystem fs,
                                                          final JobConf job,
                                                          final String name,
                                                          final Progressable progress )
    throws IOException
  {
    String clusterName = job.get( "indexer.es.cluster", "elasticsearch" );

    ElasticSearchDocumentWriter esDocWriter = new ElasticSearchDocumentWriter( );

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

    esDocWriter.setFilter( "reqFields", new RequiredFieldsFilter( ) );
    esDocWriter.setFilter( "type",      typeFilter );
    esDocWriter.setFilter( "robots",    new RobotsFilter( ) );    

    esDocWriter.setTypeNormalizer( normalizer );

    return new ElasticSearchRecordWriter( esDocWriter );
  }
  
  public class ElasticSearchRecordWriter implements RecordWriter<Text, MapWritable>
  {
    ElasticSearchDocumentWriter docWriter;
    
    public ElasticSearchRecordWriter( ElasticSearchDocumentWriter docWriter )
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
      try
        {
          docWriter.close();
        }
      catch ( IOException ioe )
        {
          // TODO: Log it.
        }
    }
  }
}
