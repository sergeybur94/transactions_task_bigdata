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

public class A9 extends Configured implements Tool {

  private static Logger logger = LoggerFactory.getLogger(A9.class);

  public static void main(final String[] args) throws Exception {
    //BasicConfigurator.configure();

    int res = ToolRunner.run(new Configuration(), new A9(), args);
    System.exit(res);
  }

  public final int run(String[] args) throws Exception {
    //Args
    //Path datePath = new Path("example");//new Path(args[0]);
    Path datePath = new Path(args[0]);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "anomalySearch_A9");

    job.setMapperClass(MapperA9.class);
    job.setReducerClass(ReducerA9.class);
    System.out.println("Seted mapper/reducer");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.out.println("Seted classes");
    job.setJarByClass(A9.class);

    //job.setNumReduceTasks(1);

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
      String ban = inputStr[0].trim();
      String subNo = inputStr[1].trim();
      context.write(new Text(subNo), value);//ban+"_"+subNo
    }
  }

  public static class ReducerA9 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      //Here is A9 searching
      List<Integer> socSeqNoList = new ArrayList<>();
      List<String> ftrSeqNoList = new ArrayList<>();
      List<String> dateList = new ArrayList<>();
      HashMap<Integer, String> dictSoc = new HashMap<>();
      long a92count = 0;
      long a91count = 0;

      for (Text value : values) {
        String[] valueRecord = value.toString().split("\u0001");
        String ban = valueRecord[0].trim();
        String subNo = valueRecord[1].trim();
        Integer socSeqNo = Integer.parseInt(valueRecord[3].trim());
        String ftrSeqNo = valueRecord[4].trim();
        String date = valueRecord[5].trim();


        socSeqNoList.add(socSeqNo);
        if (dictSoc.containsKey(socSeqNo)){
          System.out.println("A9.2: "+socSeqNo);
          context.write(key, new Text("A9.2: "+socSeqNo));
          a92count += 1;
        }
        dictSoc.put(socSeqNo, date);

        context.write(key, new Text(ban+"\t"+subNo+"\t"+socSeqNo+"\t"+ftrSeqNo+"\t"+date));
      }

      String prevDate = "";
      Integer prevSocNo = 0;
      SortedSet<Integer> keys = new TreeSet<Integer>(dictSoc.keySet());
      for (Integer socNo : keys){
        String curDate = dictSoc.get(socNo);
        if (curDate.compareTo(prevDate)<0){
          //If less
          System.out.println("A9.1: "+prevSocNo+"->"+socNo+"\t"+prevDate+"  ->  "+curDate);
          context.write(key, new Text("A9.1: "+prevSocNo+"->"+socNo+"\t"+prevDate+"  ->  "+curDate));
          a91count += 1;
        }
        prevDate = curDate;
        prevSocNo = socNo;
      }

//      if(a91count>0){
//        context.write(key, new Text("A9.1: "+a91count));
//      }
//      if(a92count>0){
//        context.write(key, new Text("A9.2: "+a92count));
//      }

      //Here starts A10 searching

    }
  }
}