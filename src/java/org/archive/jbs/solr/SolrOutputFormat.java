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

package org.archive.jbs.solr;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.archive.jbs.Document;
import org.archive.jbs.util.*;
import org.archive.jbs.filter.*;
import org.archive.jbs.*;

/**
 * This class is inspired by the technique used in Nutch's
 * LuceneOutputFormat class.  However, rather than creating a Lucene
 * index and writing documents to it, we use the SolrJ API to send the
 * documents to a (remote) Solr server.
 *
 * We perform essentially the same type normalization and filtering,
 * robot filtering, etc. before forming the Solr document.
 */
public class SolrOutputFormat extends FileOutputFormat<Text, Text>
{
  public RecordWriter<Text, Text> getRecordWriter( final FileSystem fs,
                                                   final JobConf job,
                                                   final String name,
                                                   final Progressable progress )
    throws IOException
  {
    String serverUrl  = job.get( "jbs.solr.url", "http://localhost:8983/solr" );
    int    docBufSize = job.getInt( "jbs.solr.bufSize", 10 );

    SolrDocumentWriter solrDocWriter = new SolrDocumentWriter( new URL( serverUrl ), docBufSize );

    // FIXME: Temporary collection hack
    solrDocWriter.collectionHack = job.get( "jbs.solr.collectionHack", null );

    TypeNormalizer normalizer = new TypeNormalizer( );
    Map<String,String> aliases = normalizer.parseAliases( job.get( "jbs.typeNormalizer.aliases", "" ) );

    if ( job.getBoolean( "jbs.typeNormalizer.useDefaults", true ) )
      {
        Map<String,String> defaults = normalizer.getDefaultAliases( );
        defaults.putAll( aliases );

        aliases = defaults;
      }
    normalizer.setAliases( aliases );

    TypeFilter typeFilter = new TypeFilter( );
    Set<String> allowedTypes = typeFilter.parse( job.get( "jbs.typeFilter.allowed", "" ) );

    if ( job.getBoolean( "jbs.typeFilter.useDefaults", true ) )
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
    solrDocWriter.setIDNHelper     ( buildIDNHelper( job ) );

    return new SolrRecordWriter( solrDocWriter );
  }
  
  public class SolrRecordWriter implements RecordWriter<Text, Text>
  {
    SolrDocumentWriter docWriter;
    
    public SolrRecordWriter( SolrDocumentWriter docWriter )
    {
      this.docWriter = docWriter;
    }

    public void write( Text key, Text value )
      throws IOException
    {
      this.docWriter.add( key.toString(), new Document( value.toString() ) );
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

  /**
   * Build an IDNHelper object using configuration information in the JobConf.
   */
  protected IDNHelper buildIDNHelper( JobConf job )
    throws IOException
  {
    IDNHelper helper = new IDNHelper( );

    if ( job.getBoolean( "jbs.idnHelper.useDefaults", true ) )
      {
        InputStream is = this.getClass().getClassLoader( ).getResourceAsStream( "effective_tld_names.dat" );
        
        if ( is == null )
          {
            throw new RuntimeException( "Cannot load default tld rules: effective_tld_names.dat" );
          }
        
        Reader reader = new InputStreamReader( is, "utf-8" );
       
        helper.addRules( reader );
      }

    String moreRules = job.get( "jbs.idnHelper.moreRules", "" );
    
    if ( moreRules.length() > 0 )
      {
        helper.addRules( new StringReader( moreRules ) );
      }

    return helper;
  }

}
