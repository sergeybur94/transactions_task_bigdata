package ru.beeline.anomalySearch;

//import au.com.bytecode.opencsv.CSVReader;
//import javafx.collections.transformation.SortedList;
//import org.apache.commons.csv.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
//import org.apache.hadoop.hive.ql.io.orc.*;
//import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.io.compress.*;
//import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
//import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;
//import java.util.stream.Collectors;

public class A10 extends Configured implements Tool {

  private static Logger logger = LoggerFactory.getLogger(A10.class);

  public static void main(final String[] args) throws Exception {
    //BasicConfigurator.configure();

    int res = ToolRunner.run(new Configuration(), new A10(), args);
    System.exit(res);
  }

  public final int run(String[] args) throws Exception {
    //Args
    Path datePath = new Path("example");//new Path(args[0]);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "anomalySearch_A10");

    job.setMapperClass(MapperA9.class);
    job.setReducerClass(ReducerA9.class);
    System.out.println("Seted mapper/reducer");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.out.println("Seted classes");
    job.setJarByClass(A9.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, datePath);

    Path outPath = new Path("A9_"+System.currentTimeMillis());
    FileOutputFormat.setOutputPath(job, outPath);

    fs.delete(outPath, true);
    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Ends. Duration: " + ((System.currentTimeMillis() - startTime) / 1000.0) + " sec");
    long mapInput = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
    System.out.println("Records count: "+mapInput);
    //fs.delete(outPath, true);

    return 0;
  }

  //***************START***************//
  //********ANALITYCS METHODS**********//


  //************************************//
  //***************END******************//

  public static class MapperA9 extends Mapper<Object, Text, Text, Text> {

    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {

      String[] inputStr = value.toString().split("\u0001");
      String ban = inputStr[0];
      context.write(new Text(ban), value);
    }
  }

  public static class ReducerA9 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<Integer> socSeqNoList = new ArrayList<>();
      List<String> ftrSeqNoList = new ArrayList<>();
      List<String> dateList = new ArrayList<>();
      HashMap<Integer, String> dictSoc = new HashMap<>();

      for (Text value : values) {
        String[] valueRecord = value.toString().split("\u0001");
        Integer socSeqNo = Integer.parseInt(valueRecord[3]);
        String ftrSeqNo = valueRecord[4];
        String date = valueRecord[5];

        socSeqNoList.add(socSeqNo);
        if (dictSoc.containsKey(socSeqNo)){
          context.write(key, new Text("ANOMALY_9.2: "+socSeqNo));
        }
        dictSoc.put(socSeqNo, date);

        context.write(key, new Text(socSeqNo+"\t"+ftrSeqNo+"\t"+date));
      }

      String prevDate = "";
      Integer prevSocNo = 0;
      SortedSet<Integer> keys = new TreeSet<Integer>(dictSoc.keySet());
      for (Integer socNo : keys){
        String curDate = dictSoc.get(socNo);
        if (curDate.compareTo(prevDate)<0){
          //If less
          context.write(key, new Text("ANOMALY_9.1: "+prevSocNo+"->"+socNo+"\t"+prevDate+"  ->  "+curDate));
        }
        prevDate = curDate;
        prevSocNo = socNo;
      }

//      for (Integer socNo : keys){
//        System.out.println("Key: "+socNo);
//      }
//      System.out.println("=== New ===");
//
//      List<String> resultList = new ArrayList<>();
//      long i = 0;
//      for (String seqNo : socSeqNoList){
//        String result = "";
//        result += socSeqNoList.get((int)i)+"\t"+ftrSeqNoList.get((int)i)+"\t"+dateList.get((int)i);
//        resultList.add(result);
//        i++;
//      }
//
//      resultList.sort(String.CASE_INSENSITIVE_ORDER);
//      if (resultList.size()>1){
//        for (String result : resultList){
//          context.write(key, new Text(result));
//        }
//      }
    }
  }
}
