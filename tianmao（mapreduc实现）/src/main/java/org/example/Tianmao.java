package org.example;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Streams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Tianmao {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tianmao");
        job.setJarByClass(Tianmao.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);


        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String S = value.toString();
            String[] A = S.split(",");
            int a =Integer.valueOf(A[6]);
            if (a!=0) {
                IntWritable v = new IntWritable(1);
                Text k = new Text(A[1]);
                context.write(k, v);
            }
        }
    }
        public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {


            private static TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return o2.compareTo(o1);
                }
            });

            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int result = 0;
                for (IntWritable s : values)//-----------对 text 的遍历,s是任意赋值
                {
                    result += s.get();

                }
                IntWritable v = new IntWritable(result);
                  //context.write(key, v);
                treeMap.put(new Integer(result), key.toString());
                if (treeMap.size() > 100) {
                    treeMap.remove(treeMap.lastKey());
               }
            }

            protected void cleanup(Context context)
                    throws IOException, InterruptedException {
                Set<Map.Entry<Integer, String>> set = treeMap.entrySet();

                for (Map.Entry<Integer, String> entry : set) {
                    Text v = new Text(entry.getValue());
                    IntWritable s = new IntWritable(entry.getKey());
                    context.write(v, s);
                }
            }

        }

}

