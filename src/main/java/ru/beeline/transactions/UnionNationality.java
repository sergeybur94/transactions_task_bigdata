package ru.beeline.transactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.*;

public class UnionNationality extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new UnionNationality(), args);
        System.exit(res);
    }

    public final int run(String[] args) throws Exception {
        //Args
        Path datePath = new Path(args[0]);

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ":");
        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf, "UnionNationality");

        job.setMapperClass(MapperUN.class);
        //Используем комбайнер, чтобы схлопнуть данные (суммы транзакций по месяцам) на маппере,
        //тем самым сократив объем передаваемых по сети данных
        job.setCombinerClass(ReducerUN.class);
        job.setReducerClass(ReducerUN.class);
        //job.setPartitionerClass(PartitionerUN.class);

        System.out.println("Set mapper/reducer");

        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(UnionNationality.class);
        System.out.println("Set classes");

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, datePath);

        Path outPath = new Path("Result_"+System.currentTimeMillis());
        FileOutputFormat.setOutputPath(job, outPath);

        fs.delete(outPath, true);
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Ends. Duration: " + ((System.currentTimeMillis() - startTime) / 1000.0) + " sec");

        return 0;
    }

    public static class MapperUN extends Mapper<Object, Text, Text, Text> {

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            //В маппере не будем делать проверку на заголовок, т.к. пробросить его дальше может выйти "дешевле",
            //чем в каждой записи проверять ключ (записей могут быть миллиарды)
            String[] inputStr = value.toString().split(":");
            String subNo = inputStr[0];
                try {
                    //По-умолчанию считаем, что мы на вход получаем запись вида ctn:date:sum
                    //т.к. это будет "дешевле", чем в цикле проверять if-else
                    String date = inputStr[1];
                    String sum = inputStr[2];
                    String result = date + ":" + sum;
                    context.write(new Text(subNo), new Text(result));
                } catch (Exception e) {
                    //Если в блоке try ошибка, значит получена запись вида ctn:nationality
                    String national = inputStr[1];
                    context.write(new Text(subNo), new Text(national));
                }
        }
    }

    public static class ReducerUN extends Reducer<Text, Text, Text, Text> {

        public void run(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            this.setup(context);

            //Тут заголовок с пробелом, т.к. если его делать без пробела - ломается группировка ключей. Не знаю почему.
            context.write(new Text(" ctn"), new Text("month:sum:nationality"));

            try {
                while(context.nextKey()) {
                    this.reduce(context.getCurrentKey(), context.getValues(), context);
                    Iterator<Text> iter = context.getValues().iterator();
                    if (iter instanceof ReduceContext.ValueIterator) {
                        ((ReduceContext.ValueIterator)iter).resetBackupStore();
                    }
                }
            } finally {
                this.cleanup(context);
            }

        }


        public void reduce(Text key, Iterable<Text> values, Context context) {

            if (!key.toString().contains("ctn")) {
                HashMap<String, Double> months = new HashMap<>();
                String national = null;
                boolean emptyNum = true;
                boolean emptyNat = true;

                //Заполняем данные в HashMap
                try {
                    for (Text value : values) {
                        String[] record = value.toString().split(":");
                        if (record.length == 1) {
                            national = record[0];
                            emptyNat = false;
                            continue;
                        }
                        if (emptyNat && record.length == 3) {
                            national = record[2];
                            emptyNat = false;
                        }
                        Double sum;
                        try {
                            sum = Double.parseDouble(record[1].replace(",", "."));
                        } catch (Exception e) {
                            sum = Double.parseDouble("0");
                        }

                        String month;
                        //Т.к. у нас комбайнер/редьюсер превращает дату в месяц (2018-01-21 => 01), то по-умолчанию
                        //считаем, что на вход приходит запись до преобразования. Если ошибка - то значит пришла
                        //запись после преобразования
                        try {
                            month = record[0].split("-")[1];
                        } catch (ArrayIndexOutOfBoundsException e) {
                            month = record[0];
                        }
                        if (months.containsKey(month)) {
                            Double currentSum = months.get(month);
                            currentSum += sum;
                            months.replace(month, currentSum);
                        } else {
                            months.put(month, sum);
                        }
                        emptyNum = false;
                    }

                    if (emptyNum && !emptyNat){
                        context.write(key, new Text(national));
                    } else {
                        if (emptyNat) {
                            //Сделаем небольшое дублирование кода, чтобы в цикле каждый раз не проверять if
                            //т.к. записей может быть миллионы (миллиарды)
                            for (String month : months.keySet()) {
                                String send = month + ":" + months.get(month).toString().replace(".", ",");
                                context.write(key, new Text(send));
                            }
                        } else {
                            for (String month : months.keySet()) {
                                String send = month + ":" + months.get(month).toString().replace(".", ",")+ ":" + national;
                                context.write(key, new Text(send));
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Exception: " + e.toString());
                }
            }
        }
    }

    /*
    public static class PartitionerUN extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numReducers){
            return key.hashCode()%numReducers;
        }
    }*/
}