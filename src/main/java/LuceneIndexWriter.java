import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.document.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.tartarus.snowball.ext.SpanishStemmer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by barcode on 6/10/15.
 */
public class LuceneIndexWriter {

    static final String INDEX_PATH = "indexDir";
    static final String JSON_FILE_PATH = "docs.json";
    static final String STOPWORDS_FILE_PATH = "spanish_stop.txt";

    String indexPath;
    String jsonFilePath;
    IndexWriter indexWriter = null;

    public LuceneIndexWriter(String indexPath, String jsonFilePath) {
        this.indexPath = indexPath;
        this.jsonFilePath = jsonFilePath;
    }

    public void createIndex() throws FileNotFoundException {
        JSONArray jsonObjects = parseJSONFile();
        openIndex();
        addDocuments(jsonObjects);
        finish();
    }

    public JSONArray parseJSONFile() throws FileNotFoundException {
        //Get the JSON file, in this case is in ~/resources/test.json
        InputStream jsonFile = new FileInputStream(jsonFilePath);
        Reader readerJson = new InputStreamReader(jsonFile);
        //Parse the json file using simple-json library
        Object fileObjects = JSONValue.parse(readerJson);
        JSONArray arrayObjects = (JSONArray) fileObjects;
        return arrayObjects;
    }

    public boolean openIndex() {
        try {
            InputStream stopWords = new FileInputStream(STOPWORDS_FILE_PATH);
            Reader readerStopWords = new InputStreamReader(stopWords);
            Directory dir = FSDirectory.open(new File(indexPath));
            SpanishStemmer st = new SpanishStemmer();
            CharArraySet charArraySet = SpanishAnalyzer.getDefaultStopSet();
            SpanishAnalyzer analyzer = new SpanishAnalyzer(Version.LUCENE_46, charArraySet);
            IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_46, analyzer);
            //Always overwrite the directory
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            indexWriter = new IndexWriter(dir, iwc);
            return true;
        } catch (Exception e) {
            System.err.println("Error opening the index. " + e.getMessage());
        }
        return false;
    }

    /**
     * Add documents to the index
     */
    public void addDocuments(JSONArray jsonObjects) {
        for (JSONObject object : (List<JSONObject>) jsonObjects) {
            Document doc = new Document();
            final FieldType bodyOptions = new FieldType();
            bodyOptions.setIndexed(true);
            bodyOptions.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
            bodyOptions.setStored(true);
            bodyOptions.setStoreTermVectors(true);
            bodyOptions.setTokenized(true);
            for (String field : (Set<String>) object.keySet()) {
                doc.add(new Field(field, (String) object.get(field), bodyOptions));
            }
            try {
                System.out.println(doc);
                indexWriter.addDocument(doc);
            } catch (IOException ex) {
                System.err.println("Error adding documents to the index. " + ex.getMessage());
            }
        }
    }

    /**
     * Write the document to the index and close it
     */
    public void finish() {
        try {
            indexWriter.commit();
            indexWriter.close();
        } catch (IOException ex) {
            System.err.println("We had a problem closing the index: " + ex.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {
        LuceneIndexWriter liw = new LuceneIndexWriter(INDEX_PATH, JSON_FILE_PATH);
        //liw.createIndex();
        VectorPlayer vc = new VectorPlayer();
        HashMap dictionary = vc.buildDict("/home/barcode/Documents/resultResources/out.seq");
        vc.vectorReader("/home/barcode/Documents/resultResources/out.vec", dictionary);
    }

}
