
import org.apache.lucene.document.*;


public interface FieldHandler
{
  public void handle( Document doc, DocumentProperties properties );
}
