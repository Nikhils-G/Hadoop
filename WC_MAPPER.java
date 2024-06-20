# 1. WORD COUNT MAPPER 

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Interitable;
import org.apache.hadoop. to. Longeritable; 
import org.apache.hadoop.io. Text;
import org.apache.podoop.mapred. MapReduceñase;
import org.apache.hadoop.mapred.Rapper
import org.apache.hadoop.mapred. OutputCollector;
lMport org.apache.hadoop.mapred. Reporter;
public class WC Rapper extends MapReduceBase implementsMapper (LongWritable, Text, Text, IntWritable>
private final static Intwritable one = new Intwritable;
private Text word = new Text();
public void map (LongWritable key, Textvalue, OutputCollectorsText, IntWritable> output, Reporter reporter) throws IOException{
String Line value.toString(); 
StringTokenizer tokenizer new StringTokenizer (Line); 
while (tokenizer.hasMoreTokens()){ word.set(tokenizer.nextToken()); output.collect/word, one);
}
}
}
