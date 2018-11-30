package ru.beeline.anomalySearch;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.csv.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class A9 extends Configured implements Tool {

  private static Logger logger = LoggerFactory.getLogger(A9.class);

  public static void main(final String[] args) throws Exception {
    //BasicConfigurator.configure();

    int res = ToolRunner.run(new Configuration(), new A9(), args);
    System.exit(res);
  }

  public final int run(String[] args) throws Exception {
    //Args
    Path datePath = new Path("example");//new Path(args[0]);

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
    fs.delete(outPath, true);

    return 0;
  }

  //***************START***************//
  //********ANALITYCS METHODS**********//


  //************************************//
  //***************END******************//

  public static class MapperA9 extends Mapper<Object, Text, Text, Text> {

    public void map(final Object key, final Text value, final Context context) {

      String[] inputStr = value.toString().split("\u0001");

    }
  }

  public static class ReducerA9 extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {
      for (Text value : values) {
        context.write(key, value);
      }
    }
  }

}