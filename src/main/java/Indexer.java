import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

/**
 * Created by barcode on 6/10/15.
 */
public class Indexer {

    static final String INDEX_PATH = "indexDir";
    static final String JSON_FILE_PATH = "~/test.json";

    @Test
    public void testWriteIndex() {
        try {
            LuceneIndexWriter lw = new LuceneIndexWriter(INDEX_PATH, JSON_FILE_PATH);
            lw.createIndex();

//Check the index has been created successfully
            Directory indexDirectory = FSDirectory.open(new File(INDEX_PATH));
            IndexReader indexReader = DirectoryReader.open(indexDirectory);

            int numDocs = indexReader.numDocs();
            Assert.assertEquals(numDocs, 3);

            for (int i = 0; i < numDocs; i++) {
                Document document = indexReader.document(i);
                System.out.println("d=" + document);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testQueryLucene() throws IOException, ParseException {
        Directory indexDirectory = FSDirectory.open(new File(INDEX_PATH));
        IndexReader indexReader = DirectoryReader.open(indexDirectory);
        final IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        Term t = new Term("name", "id2");
        Query query = new TermQuery(t);
        TopDocs topDocs = indexSearcher.search(query, 10);
        Assert.assertEquals(1, topDocs.totalHits);
    }
}
