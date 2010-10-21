
import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.lucene.analysis.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class LuceneDocumentWriter extends DocumentWriterBase
{
  private IndexWriter indexer;
  private Analyzer    analyzer;

  private Map<String,FieldHandler> handlers;
  
  public LuceneDocumentWriter( IndexWriter indexer )
  {
    this.indexer  = indexer;
    this.analyzer = indexer.getAnalyzer();
  }

  public LuceneDocumentWriter( IndexWriter indexer, Analyzer analyzer )
  {
    this.indexer  = indexer;
    this.analyzer = analyzer;
  }

  public void setHandlers( Map<String,FieldHandler> handlers )
  {
    this.handlers = handlers;
  }

  public void add( String key, DocumentProperties properties )
    throws IOException
  {
    for ( DocumentFilter filter : filters.values() )
      {
        if ( ! filter.isAllowed( properties ) )
          {
            return ;
          }
      }

    Document doc = new Document();

    for ( FieldHandler handler : handlers.values() )
      {
        handler.handle( doc, properties );
      }

    indexer.addDocument( doc, analyzer );
  }
 
}
