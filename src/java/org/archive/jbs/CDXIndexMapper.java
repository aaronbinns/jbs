/**
 * Copyright 2013 Internet Archive
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

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;

import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.resourceindex.cdx.CDXFormatIndex;
import org.archive.wayback.resourceindex.cdx.SearchResultToCDXFormatAdapter;
import org.archive.wayback.resourceindex.cdx.format.CDXFormat;
import org.archive.wayback.resourceindex.cdx.format.CDXFormatException;
import org.archive.wayback.resourcestore.indexer.ArcIndexer;
import org.archive.wayback.resourcestore.indexer.IndexWorker;
import org.archive.wayback.resourcestore.indexer.WarcIndexer;
import org.archive.wayback.util.url.KeyMakerUrlCanonicalizer;
import org.archive.wayback.UrlCanonicalizer;


public class CDXIndexMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
{
  public static final Log LOG = LogFactory.getLog( CDXIndexMapper.class );

  JobConf job;
  
  ArcIndexer  arcIndexer  = new ArcIndexer();
  WarcIndexer warcIndexer = new WarcIndexer();

  public void configure( JobConf job )
  {
    this.job = job;

    // Default is "classic" style, not surt.
    boolean surt = this.job.getBoolean( "jbs.cdx.surt", false );

    this.arcIndexer .setCanonicalizer( new KeyMakerUrlCanonicalizer( surt ) );
    this.warcIndexer.setCanonicalizer( new KeyMakerUrlCanonicalizer( surt ) );
  }

  public CloseableIterator<CaptureSearchResult> indexFile( String path, InputStream is )
    throws IOException 
  {
    CloseableIterator<CaptureSearchResult> itr = null;
    
    if ( path.endsWith( IndexWorker.ARC_EXTENSION ) ) 
      {
        itr = arcIndexer.iterator( (ARCReader) ARCReaderFactory.get( path, is, true ) );
      }
    else if ( path.endsWith( IndexWorker.ARC_GZ_EXTENSION ) )
      {
        itr = arcIndexer.iterator( (ARCReader) ARCReaderFactory.get( path, is, true ) );
      }
    else if ( path.endsWith( IndexWorker.WARC_EXTENSION ) )
      {
        itr = warcIndexer.iterator( (WARCReader) WARCReaderFactory.get( path, is, true ) );
      }
    else if ( path.endsWith( IndexWorker.WARC_GZ_EXTENSION ) )
      {
        itr = warcIndexer.iterator( (WARCReader) WARCReaderFactory.get( path, is, true ) );
      }

    return itr;
  }


  public void map( Text key, Text value, OutputCollector output, Reporter reporter )
    throws IOException
  {
    String path = key.toString();
    
    LOG.info( "Start: "  + path );
    
    FSDataInputStream fis = null;
    try
      {
        fis = FileSystem.get( new java.net.URI( path ), this.job ).open( new Path( path ) );

        CloseableIterator<CaptureSearchResult> itr = indexFile(path, fis);

        CDXFormat cdxFormat = new CDXFormat(CDXFormatIndex.CDX_HEADER_MAGIC);

        Iterator<String> lines = SearchResultToCDXFormatAdapter.adapt(itr, cdxFormat);

        while( lines.hasNext( ) )
          {
            output.collect( new Text( lines.next() ), null );
          }
      }
    catch ( Exception e )
      {
        if ( this.job.getBoolean( "jbs.cdx.abortOnArchiveReadError", true ) )
          {
            throw new IOException( e );
          }
      }
    finally 
      {
        if ( fis != null )
          {
            fis.close();
          }
      }
    
  }

}
