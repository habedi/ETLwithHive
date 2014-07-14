
//PRINTING XML ELEMENTS N OUTPUTTING IT IN XML FORMAT
//INPUT IS AN XML FILE
// <configuration>
//     <property>
//             <name>dfs.replication</name>
//             <value>1</value>
//      </property>
//     <property>
//         <name>dfs</name>
//         <value>2</value>
//     </property>
//  </configuration>


package com.org;
import javax.xml.stream.XMLStreamConstants;//XMLInputFactory;
import java.io.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import javax.xml.stream.*;

public class XmlParser11
{



        public static class XmlInputFormat1 extends TextInputFormat {

        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";


        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new XmlRecordReader();
        }

        /**
* XMLRecordReader class to read through a given xml document to output
* xml blocks as records as specified by the start tag and end tag
*
*/
    //     @Override
        public static class XmlRecordReader extends
                RecordReader<LongWritable, Text> {
            private byte[] startTag;
            private byte[] endTag;
            private long start;
            private long end;
            private FSDataInputStream fsin;
            private DataOutputBuffer buffer = new DataOutputBuffer();

            private LongWritable key = new LongWritable();
            private Text value = new Text();
                @Override
            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
                endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
                FileSplit fileSplit = (FileSplit) split;

                // open the file and seek to the start of the split
                start = fileSplit.getStart();
                end = start + fileSplit.getLength();
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                fsin = fs.open(fileSplit.getPath());
                fsin.seek(start);

            }
        @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                if (fsin.getPos() < end) {
                    if (readUntilMatch(startTag, false)) {
                        try {
                            buffer.write(startTag);
                            if (readUntilMatch(endTag, true)) {
                                key.set(fsin.getPos());
                                value.set(buffer.getData(), 0,
                                        buffer.getLength());
                                return true;
                            }
                        } finally {
                            buffer.reset();
                        }
                    }
                }
                return false;
            }
        @Override
           public LongWritable getCurrentKey() throws IOException,
                    InterruptedException {
                return key;
            }

        @Override
            public Text getCurrentValue() throws IOException,
                    InterruptedException {
                return value;
            }
        @Override
            public void close() throws IOException {
                fsin.close();
            }
        @Override
            public float getProgress() throws IOException {
                return (fsin.getPos() - start) / (float) (end - start);
            }

            private boolean readUntilMatch(byte[] match, boolean withinBlock)
                    throws IOException {
                int i = 0;
                while (true) {
                    int b = fsin.read();
                    // end of file:
                    if (b == -1)
                        return false;
                    // save to buffer:
                    if (withinBlock)
                        buffer.write(b);
                    // check if we're matching:
                    if (b == match[i]) {
                        i++;
                        if (i >= match.length)
                            return true;
                    } else
                        i = 0;
                    // see if we've passed the stop point:
                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
                        return false;
                }
            }
        }
    }


        public static class Map extends Mapper<LongWritable, Text,
    Text, Text> {
  @Override
  protected void map(LongWritable key, Text value,
                     Mapper.Context context)
      throws
      IOException, InterruptedException {
    String document = value.toString();
    System.out.println("'" + document + "'");
        try {
      XMLStreamReader reader =
          XMLInputFactory.newInstance().createXMLStreamReader(new
              ByteArrayInputStream(document.getBytes()));
      String propertyName = "";
      String propertyValue = "";
      String currentElement = "";
      while (reader.hasNext()) {
        int code = reader.next();
        switch (code) {
          case XMLStreamConstants.START_ELEMENT: //START_ELEMENT:
            currentElement = reader.getLocalName();
            break;
          case XMLStreamConstants.CHARACTERS: //CHARACTERS:
            if (currentElement.equalsIgnoreCase("uid")) {
              propertyName += reader.getText().trim();
              System.out.println(propertyName);
            } else if (currentElement.equalsIgnoreCase("location")) {
              propertyValue += reader.getText().trim();
              System.out.println(propertyValue);
            
            } else if (currentElement.equalsIgnoreCase("age")) {
              propertyValue += ("," +reader.getText().trim());
              System.out.println(propertyValue);
            }
            break;
        }
      }
      reader.close();
      context.write(new Text(propertyName.trim()), new Text(propertyValue.trim()));
     
    }
        catch(Exception e){
                throw new IOException(e);

                }

  }
}
public static class Reduce
    extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void setup(
      Context context)
      throws IOException, InterruptedException {
   // context.write(new Text("<users_info>"), null);
  }

  @Override
  protected void cleanup(
      Context context)
      throws IOException, InterruptedException {
   // context.write(new Text("</users_info>"), null);
  }

  private Text outputKey = new Text();
  public void reduce(Text key, Iterable<Text> values,
                     Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      outputKey.set(constructPropertyXml(key, value));
      context.write(outputKey, null);
    }
  }

  public static String constructPropertyXml(Text name, Text value) {
    String [] loage = value.toString().split(",");
    StringBuilder sb = new StringBuilder();
//     sb.append("<user><uid>").append(name)
//         .append("</uid><location>").append(loage[0].trim())
//         .append("</location><age>").append(loage[1].trim()).append("</age></user>");

 sb.append(name)
        .append(",").append(loage[0].trim())
        .append(",").append(loage[1].trim());
        
    return sb.toString();
  }
}



        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                 
                 conf.addResource(new Path("/HADOOP_HOME/etc/hadoop/core-site.xml"));
                 conf.addResource(new Path("/HADOOP_HOME/etc/hadoop/hdfs-site.xml"));

                conf.set("xmlinput.start", "<user>");
                conf.set("xmlinput.end", "</user>");
                Job job = new Job(conf);
                job.setJarByClass(XmlParser11.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapperClass(XmlParser11.Map.class);
                job.setReducerClass(XmlParser11.Reduce.class);

                job.setInputFormatClass(XmlInputFormat1.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.waitForCompletion(true);
        }
}

