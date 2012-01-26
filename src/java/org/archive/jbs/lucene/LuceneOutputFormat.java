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

package org.archive.jbs.lucene;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.util.*;
import org.apache.lucene.store.*;

import org.archive.jbs.Document;
import org.archive.jbs.*;
import org.archive.jbs.util.*;
import org.archive.jbs.filter.*;

/**
 * This class is inspired by Nutch's LuceneOutputFormat class.  It does
 * primarily three things of interest:
 *
 *  1. Creates a Lucene index in a local (not HDFS) ${temp} directory,
 *     into which the documents are added.
 *
 *  2. Creates a LuceneDocumentWriter and delegates creation and
 *     indexing of Lucene documents to it.
 *
 *  2. Closes that index and copies it into HDFS.
 */
public class LuceneOutputFormat extends FileOutputFormat<Text, Text>
{
  public FileSystem fs;
  public JobConf job;
  public Path temp;
  public Path perm;
  
  public IndexWriter indexer;

  public RecordWriter<Text, Text> getRecordWriter( final FileSystem fs,
                                                   final JobConf job,
                                                   final String name,
                                                   final Progressable progress )
    throws IOException
  {
    // Open Lucene index in ${temp}
    this.fs   = FileSystem.get(job);
    this.job  = job;
    this.perm = new Path( FileOutputFormat.getOutputPath( job ), name );
    this.temp = job.getLocalPath( "index/_"  + (new Random().nextInt()) );

    this.fs.delete( perm, true ); // delete old, if any

    indexer = new IndexWriter( new NIOFSDirectory( new File( fs.startLocalOutput( perm, temp ).toString( ) ) ),
                               new KeywordAnalyzer( ),
                               IndexWriter.MaxFieldLength.UNLIMITED );
    
    indexer.setMergeFactor      ( job.getInt("jbs.lucene.mergeFactor", 100) );
    indexer.setMaxMergeDocs     ( job.getInt("jbs.lucene.maxMergeDocs", Integer.MAX_VALUE) );
    indexer.setRAMBufferSizeMB  ( job.getInt("jbs.lucene.maxRAMBufferSize", 512) );
    indexer.setTermIndexInterval( job.getInt("jbs.lucene.termIndexInterval", IndexWriterConfig.DEFAULT_TERM_INDEX_INTERVAL) );
    indexer.setMaxFieldLength   ( job.getInt("jbs.lucene.max.tokens", Integer.MAX_VALUE) );
    indexer.setUseCompoundFile  ( false );
    indexer.setSimilarity       ( new WebSimilarity( ) );

    LuceneDocumentWriter docWriter = buildDocumentWriter( job, indexer );
    
    return new LuceneRecordWriter( docWriter );
  }

  public class LuceneRecordWriter implements RecordWriter<Text, Text>
  {
    LuceneDocumentWriter docWriter;

    public LuceneRecordWriter( LuceneDocumentWriter docWriter )
      throws IOException
    {
      this.docWriter = docWriter;
    }

    /**
     * Delegate to docWriter.
     */
    public void write( Text key, Text value )
      throws IOException
    {
      this.docWriter.add( key.toString(), new Document( value.toString() ) );
    }

    /**
     * Closing the LuceneRecordWriter closes the Lucene index,
     * optionally optimizes it and finally copies it into HDFS.
     */
    public void close( Reporter reporter )
      throws IOException
    {
      // Optimize and close the IndexWriter
      if ( job.getBoolean( "jbs.lucene.optimize", true ) )
        {
          indexer.optimize();
        }
      indexer.close();
      
      // Copy the index from ${temp} to HDFS and touch a "done" file.
      fs.completeLocalOutput( perm, temp );
      fs.createNewFile( new Path( perm, "done" ) );
    }
    
  }
  
  /**
   * Factory method which constructs the LuceneDocumentWriter.  Much
   * of the configuration can be controlled via the Hadoop JobConf.
   */
  protected LuceneDocumentWriter buildDocumentWriter( JobConf job, IndexWriter indexer )
    throws IOException
  {
    CustomAnalyzer analyzer = new CustomAnalyzer( job.getBoolean( "jbs.lucene.analyzer.custom.omitNonAlpha", true ),
                                                  new HashSet<String>( Arrays.asList( job.get( "jbs.lucene.analyzer.stopWords", "" ).trim().split( "\\s+" ) ) ) );
    
    LuceneDocumentWriter writer = new LuceneDocumentWriter( indexer, analyzer );

    IDNHelper      idnHelper  = buildIDNHelper( job );
    TypeNormalizer normalizer = buildTypeNormalizer( job );
    TypeFilter     typeFilter = buildTypeFilter( job, normalizer );

    writer.setFilter( "reqFields", new RequiredFieldsFilter( ) );
    writer.setFilter( "type",      typeFilter );
    writer.setFilter( "robots",    new RobotsFilter( ) );
    writer.setFilter( "http",      new HTTPStatusCodeFilter( job.get( "jbs.httpStatusCodeFilter" ) ) );

    int textMaxLength = job.getInt( "jbs.lucene.text.maxlength", TextHandler.MAX_LENGTH );

    Map<String,FieldHandler> handlers = new HashMap<String,FieldHandler>( );
    handlers.put( "url"        , new SimpleFieldHandler( "url",         Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "digest"     , new SimpleFieldHandler( "digest",      Field.Store.YES, Field.Index.NO       ) );
    handlers.put( "title"      , new SimpleFieldHandler( "title",       Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "keywords"   , new SimpleFieldHandler( "keywords",    Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "description", new SimpleFieldHandler( "description", Field.Store.YES, Field.Index.ANALYZED ) );
    handlers.put( "length"     , new SimpleFieldHandler( "length",      Field.Store.YES, Field.Index.NO ) );
    handlers.put( "collection" , new SimpleFieldHandler( "collection",  Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
    handlers.put( "code"       , new SimpleFieldHandler( "code",        Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS ) );
    handlers.put( "content"    , new TextHandler( "content", textMaxLength ) );
    handlers.put( "boiled"     , new TextHandler( "boiled" , textMaxLength ) );
    handlers.put( "date"       , new DateHandler( ) );
    handlers.put( "site"       , new SiteHandler( idnHelper ) );
    handlers.put( "type"       , new TypeHandler( normalizer ) );  
    handlers.put( "boost"      , new BoostHandler( ) );

    writer.setHandlers( handlers );
    
    return writer;
  }

  /**
   * Build a TypeNormalizer object using configuration information in the JobConf.
   */
  protected TypeNormalizer buildTypeNormalizer( JobConf job )
  {
    TypeNormalizer normalizer = new TypeNormalizer( );

    Map<String,String> aliases = normalizer.parseAliases( job.get( "jbs.typeNormalizer.aliases", "" ) );

    if ( job.getBoolean( "jbs.typeNormalizer.useDefaults", true ) )
      {
        Map<String,String> defaults = normalizer.getDefaultAliases( );
        defaults.putAll( aliases );

        aliases = defaults;
      }
    normalizer.setAliases( aliases );

    return normalizer;
  }

  /**
   * Build a TypeFilter object using configuration information in the JobConf.
   */
  protected TypeFilter buildTypeFilter( JobConf job, TypeNormalizer normalizer )
  {
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
    
    return typeFilter;
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
        InputStream is = SiteHandler.class.getClassLoader( ).getResourceAsStream( "effective_tld_names.dat" );
        
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
