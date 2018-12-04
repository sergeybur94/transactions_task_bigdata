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
import org.apache.log4j.BasicConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class InRecordAnomalies extends Configured implements Tool {

  private static Logger logger = LoggerFactory.getLogger(InRecordAnomalies.class);

  public static void main(final String[] args) throws Exception {
    //BasicConfigurator.configure();

    int res = ToolRunner.run(new Configuration(), new InRecordAnomalies(), args);
    System.exit(res);
  }

  public final int run(String[] args) throws Exception {
    //Args
    Path datePath = new Path("example");//new Path(args[0]);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "anomalySearch_InRecordAnomalies");

    job.setMapperClass(MapperInRecord.class);
    System.out.println("Seted mapper/reducer");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    System.out.println("Seted classes");
    job.setJarByClass(InRecordAnomalies.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, datePath);

    Path outPath = new Path("InRecordAnomalies_"+System.currentTimeMillis());
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

  private static HashMap<String, List<String[]>> parseSOC(String socPathString) throws IOException {

    CSVReader reader = new CSVReader(new FileReader(socPathString), '\u0001');
    List<String[]> socRecords = reader.readAll();

    HashMap<String, List<String[]>> result = new HashMap<>();
    for (String[] record : socRecords) {
      String key = record[0];
      if (!result.containsKey(key)){
        List<String[]> tempList = new ArrayList<>();
        tempList.add(record);
        result.put(key, tempList);
      } else {
        result.get(key).add(record);
      }
    }
    System.out.println("SOCs loaded");
    return result;
  }


  //***************START***************//
  //********ANALITYCS METHODS**********//

  private static boolean isA1(HashMap<String, List<String[]>> socs, String[] SFrecord){

    // If soc not exists in amdocs.soc
    // Maybe check more
    String soc = SFrecord[2];
    if (socs.containsKey(soc)) {
      List<String[]> result = socs.get(soc);
      if (result.size() < 1) {
        System.out.println("Anomaly A1. Amdocs.soc haven't this soc: "+soc);
        return true;
      }
    }
    return false;
  }

  private static boolean isA2(String[] SFrecord){

    // If sys_update_date < sys_creation_date
    String sysCreationDate = SFrecord[5];
    String sysUpdateDate = SFrecord[6];
    if (sysCreationDate.compareTo(sysUpdateDate)>0) { //< == -1, >==1
        System.out.println("Anomaly A2. Creation date > Update date: "+sysCreationDate+" > "+sysUpdateDate);
        return true;
    }
    return false;
  }

  private static boolean isA3(String[] SFrecord){

    // If soc_effective_date > sys_creation_date
    String sysCreationDate = SFrecord[5];
    String socEffectiveDate = SFrecord[11];
    if (sysCreationDate.compareTo(socEffectiveDate)<0) { //a<b == -1, a>b == 1, a==b ==0
      System.out.println("Anomaly A3. Creation date < Soc effective date: "+sysCreationDate+" < "+socEffectiveDate);
      return true;
    }
    return false;
  }

  private static boolean isA4(String[] SFrecord){

    // If operator_id == NULL & application_id == NULL
    // or
    // operator_id != NULL & application_id != NULL
    // NULL == \N
    String operatorId = SFrecord[7];
    String applicationId = SFrecord[8];
    if (operatorId.equals("\\N") & applicationId.equals("\\N")) {
      System.out.println("Anomaly A4.1. Operator ID is NULL and Application ID is NULL: "
          +operatorId+" , "+applicationId);
      return true;
    }
    if (!operatorId.equals("\\N") & !applicationId.equals("\\N")) {
      System.out.println("Anomaly A4.2. Operator ID not NULL and Application ID not NULL: "
          +operatorId+" , "+applicationId);
      return true;
    }
    return false;
  }

  private static boolean isA5(String[] SFrecord){

    // If ban_src != customer_id
    String banSrc = SFrecord[0];
    //System.out.println("Ban_src: "+banSrc);
    //System.out.println("Poor ID: "+SFrecord[12]);
    String customerId = SFrecord[12].split("E")[0].replace(".", "");
    while (customerId.length() < 9){
      customerId += "0";
    }
    //System.out.println("Customer ID: "+customerId);
    if (!banSrc.equals(customerId)) {
      System.out.println("Anomaly A5. Ban_src != Customer ID: "
          +banSrc+" != "+customerId);
      return true;
    }
    return false;
  }

  private static boolean isA6(String[] SFrecord){

    // If ftr_effective_date < soc_effective_date
    String socEffectiveDate = SFrecord[11];
    String ftrEffectiveDate = SFrecord[16];
    if (socEffectiveDate.compareTo(ftrEffectiveDate)>0) { //< == -1, >==1
      System.out.println("Anomaly A6. Ftr effective date < Soc effective: "+ftrEffectiveDate+" < "+socEffectiveDate);
      return true;
    }
    return false;
  }

  private static boolean isA7(String[] SFrecord){

    // If ftr_effective_date > ftr_expiration_date
    String ftrEffectiveDate = SFrecord[16];
    String ftrExpirationDate = SFrecord[18];
    if (ftrExpirationDate.compareTo(ftrEffectiveDate)<0) { //< == -1, >==1
      System.out.println("Anomaly A7. Ftr effective date > Ftr Expiration date: "+ftrEffectiveDate+" > "+ftrExpirationDate);
      return true;
    }
    return false;
  }

  private static boolean isA8(String[] SFrecord){

    // If ftr_effective_date > ftr_expiration_date
    String serviceType = SFrecord[14];
    String serviceClass = SFrecord[31];
    if (serviceClass.equals("SOC") & !serviceType.equals("O")) { //< == -1, >==1
      System.out.println("Anomaly A8.1. SOC != O: "+serviceClass+" != "+serviceType);
      return true;
    }
    if (serviceClass.equals("PP") & !serviceType.equals("P")) { //< == -1, >==1
      System.out.println("Anomaly A8.2. PP != P: "+serviceClass+" != "+serviceType);
      return true;
    }
    if ((!serviceClass.equals("SOC") & !serviceClass.equals("PP")) |
        (!serviceType.equals("P") & !serviceType.equals("O"))) { //< == -1, >==1
      System.out.println("Anomaly A8.3. Unknown service_class or service_type: "+serviceClass+" , "+serviceType);
      return true;
    }
    return false;
  }
  //************************************//
  //***************END******************//

  public static class MapperInRecord extends Mapper<Object, Text, Text, Text> {

    HashMap<String, List<String[]>> socs = parseSOC("socs_dir/socs");
    long a1count = 0;
    long a2count = 0;
    long a3count = 0;
    long a4count = 0;
    long a5count = 0;
    long a6count = 0;
    long a7count = 0;
    long a8count = 0;

    public MapperInRecord() throws IOException {
    }

    public void map(final Object key, final Text value, final Context context) {

      String[] inputStr = value.toString().split("\u0001");
      if(isA1(socs, inputStr)){
        // If soc not exists in amdocs.soc
        a1count++;
        System.out.println("A1 count: "+a1count);
      }
      if(isA2(inputStr)){
        // If sys_update_date < sys_creation_date
        a2count++;
        System.out.println("A2 count: "+a2count);
      }
      if(isA3(inputStr)){
        // If soc_effective_date > sys_creation_date
        a3count++;
        System.out.println("A3 count: "+a3count);
      }
      if(isA4(inputStr)){
        // If operator_id == NULL & application_id == NULL
        // or
        // operator_id != NULL & application_id != NULL
        a4count++;
        System.out.println("A4 count: "+a4count);
      }
      if(isA5(inputStr)){
        // If ban_src != customer_id
        a5count++;
        System.out.println("A5 count: "+a5count);
      }
      if(isA6(inputStr)){
        // If ftr_effective_date < soc_effective_date
        a6count++;
        System.out.println("A6 count: "+a6count);
      }
      if(isA7(inputStr)){
        // If ftr_effective_date > ftr_expiration_date
        a7count++;
        System.out.println("A7 count: "+a7count);
      }
      if(isA8(inputStr)){
        // If service_class != service_type
        a8count++;
        System.out.println("A8 count: "+a8count);
      }
    }
  }

  public static class RndPartitioner extends Partitioner < Writable, Writable > {
    @Override
    public int getPartition(Writable key, Writable value, int numReduceTasks){
      Random random = new Random();
      int rndPartition = random.nextInt(numReduceTasks);
      return rndPartition;
    }
  }

}