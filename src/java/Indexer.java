import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.metadata.Metadata;


public class Indexer 
{

  public static class Map extends MapReduceBase implements Mapper<Text, Writable, Text, MapWritable>
  {
    public void map( Text key, Writable value, OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException
    {
      MapWritable m = new MapWritable( );

      if ( value instanceof ParseData )
        {
          ParseData pd = (ParseData) value;

          put( m, "title", pd.getTitle( ) );

          Metadata meta = pd.getContentMeta( );
          for ( String name : meta.names( ) )
            {
              put( m, name, meta.get( name ) );
            }
        }
      else if ( value instanceof ParseText )
        {
          put( m, "content_parsed", value.toString() );
        }
      else
        {
          // Weird
          return ;
        }

      output.collect( key, m );
    }

    private void put( MapWritable m, String key, String value )
    {
      if ( value == null ) value = "";

      m.put( new Text( key ), new Text( value ) );
    }

  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, MapWritable, Text, MapWritable> 
  {
    public void reduce( Text key, Iterator<MapWritable> values, OutputCollector<Text, MapWritable> output, Reporter reporter)
      throws IOException
    {
      MapWritable m = new MapWritable( );

      // FIXME: This looks pretty bogus...just merging all the
      // mappings from the MapWritables into one.  We should take the
      // [k,v] pairs from each MapWritable and merge them
      // intelligently.  In particular the dates.  E.g. we might have
      //   MapWritable1 = [ "date" => "20100501..."
      //   MapWritable2 = [ "date" => "20081219..."
      // We want to have
      //   Merged       = [ "date" => ["20100501...","20081219..."]
      // with one key "date" and multiple values.
      // The way the below code acts is just to add both the mappings
      // to the Merged MapWritable, which means only the last one will
      // be kept.
      while ( values.hasNext( ) )
        {
          m.putAll( values.next( ) );
        }
      
      output.collect( key, m );
    }
  }
  
  public static void main(String[] args) throws Exception
  {
    if (args.length != 2)
      {
        System.err.println( "Indexer <input> <output>" );
        System.exit(1);
      }
      
    JobConf conf = new JobConf(Indexer.class);
    conf.setJobName("Indexer");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(MapWritable.class);
    
    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);
    
    conf.setInputFormat(SequenceFileInputFormat.class);

    // LuceneOutputFormat writes to Lucene index.
    conf.setOutputFormat(LuceneOutputFormat.class);
    // For debugging, sometimes easier to inspect Hadoop mapfile format.
    // conf.setOutputFormat(MapFileOutputFormat.class);
    
    // Assume arg[0] is a Nutch(WAX) segment
    Path base = new Path( args[0] );
    FileInputFormat.addInputPath(conf, new Path( base, "parse_data"));
    FileInputFormat.addInputPath(conf, new Path( base, "parse_text"));

    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    JobClient.runJob(conf);
  }

}
