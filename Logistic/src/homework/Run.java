/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/*
arr[0] : số lượng feature
arr[1] : learing rate
arr[2] : số lần lặp
arr[3] : input
arr[4] : output
 */
public class Run {

    public static int num_features;
    public static float lr;

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        num_features = Integer.parseInt(args[0]);
        ++num_features; // 117
        lr = Float.parseFloat(args[1]);

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Float[] W = new Float[num_features];

        File file = new File("D:\\log.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
            if (i == 0) {
                for (int j = 1; j < num_features; j++) {
                    W[j] = (float) 0;
                }
            } else {
                BufferedReader br1 = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[4] + "/part-r-00000"))));
                String line1;

                while ((line1 = br1.readLine()) != null) {
                    String[] theta_line = line1.split("\t");
                    int index = Integer.parseInt(theta_line[0].replaceAll("W", ""));
                    W[index] = Float.parseFloat(theta_line[1]);
                }

                br1.close();
            }

            if (hdfs.exists(new Path(args[4]))) {
                hdfs.delete(new Path(args[4]), true);
            }

//            hdfs.close();
            conf.setFloat("lr", lr);
            // W1->W116
            for (int j = 1; j < num_features; j++) {
                conf.setFloat("W" + j, W[j]);
            }
            Job job = Job.getInstance(conf, " logistic hadoop");
            job.setJarByClass(Run.class);

            FileInputFormat.setInputPaths(job, new Path(args[3]));
            job.setMapperClass(logisMap.class);
            job.setReducerClass(logisReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            FileOutputFormat.setOutputPath(job, new Path(args[4]));
            job.waitForCompletion(true);

            // tính acc
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfs.open(new Path(args[5] + "/dota2Test.csv"))));
            FileOutputStream fileOutputStream;
            if (i == 0) {
                fileOutputStream = new FileOutputStream(file, false);
            } else {
                fileOutputStream = new FileOutputStream(file, true);
            }

            String line;

            int correct = 0, total = 0;
            Float[] X = new Float[1000];

            while ((line = bufferedReader.readLine()) != null) {
                String[] tmp = line.split("\\,");
                for (int j = 1; j < tmp.length; j++) {
                    X[j] = Float.parseFloat(tmp[j]);
                }

                float sum = 0;

                for (int j = 1; j < tmp.length; j++) {
                    sum += (X[j] * W[j]);
                }

                float predict = (float) (1 / (1 + (Math.exp(-sum))));

                float Yi = Float.parseFloat(tmp[0]);

                if ((predict >= 0.5 && Yi == 1) || (predict < 0.5 && Yi == 0)) {
                    correct++;
                }
                total++;
            }

            bufferedReader.close();
            fileOutputStream.write(("\n Loop " + i + " acc = " + (float) correct / total + "\t\t").getBytes());
            for (int j = 1; j < num_features; j++) {
                fileOutputStream.write(("W" + j + ": " + W[j] + "\t").getBytes());
            }
            fileOutputStream.close();
        }

        hdfs.close();
    }
}
