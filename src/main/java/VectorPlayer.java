import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Created by barcode on 10/10/15.
 */
public class VectorPlayer {

    public static void main(String[] args) throws IOException {
       /* HashMap dictionary = buildDict("/home/barcode/Documents/resultResources/out.seq");
        vectorReader("/home/barcode/Documents/resultResources/out.vec", dictionary);*/
    }


    public void vectorReader(String vectorPath, HashMap dict) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(vectorPath);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        LongWritable key = new LongWritable();
        VectorWritable value = new VectorWritable();
        HashMap dictionaryMap = dict;
        while (reader.next(key, value))

        {
            NamedVector namedVector = (NamedVector) value.get();
            RandomAccessSparseVector vect = (RandomAccessSparseVector) namedVector.getDelegate();
            Iterator<Vector.Element> itVe = vect.iterator();

            while (itVe.hasNext()) {
                Vector.Element e = itVe.next();
                if (e.get() > 0.0) {
                    System.out.println("Token: " + dictionaryMap.get(e.index()) + ", TF-IDF weight: " + e.get());
                }
            }

        }

        reader.close();
    }

    public HashMap buildDict(String dictUrl) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader read = new SequenceFile.Reader(fs, new Path(dictUrl), conf);
        IntWritable dicKey = new IntWritable();
        Text text = new Text();
        HashMap dictionaryMap = new HashMap();
        while (read.next(text, dicKey)) {
            dictionaryMap.put(Integer.parseInt(dicKey.toString()), text.toString());
        }
        read.close();
        return dictionaryMap;
    }
}
