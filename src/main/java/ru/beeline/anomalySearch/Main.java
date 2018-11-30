package ru.beeline.anomalySearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Main extends Configured implements Tool {
  public static void main(final String[] args) throws Exception {
    //BasicConfigurator.configure();
    Runtime.getRuntime().addShutdownHook(new Thread() {

      private void shutdown() throws IOException {
        System.out.println("Shutdown Main");
      }

      public void run() {
        try{
          shutdown();
        } catch (IOException e){

        }
      }
    });

    int res = ToolRunner.run(new Configuration(), new Main(), args);
    System.exit(res);
  }

  public final int run(String[] args) throws Exception {

    InRecordAnomalies inRecord = new InRecordAnomalies();
    try {
      System.out.println("Path: "+args[0]);
      inRecord.main(args);
    } catch (Exception | Error e){
      System.out.println("Err: "+e.getMessage());
    }
    System.out.println("SFUHSF");
    return 0;
  }

}
