/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package homework;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class logisMap extends Mapper<LongWritable, Text, Text, FloatWritable> {

    public static int count = 0;
    public static float lr = 0.0f;
    public static Float[] Xi;
    public static ArrayList<Float> Wi = new ArrayList<Float>();

    @Override
    public void setup(Context context) {
        lr = context.getConfiguration().getFloat("lr", 0);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) {
        ++count;
        float predic = 0;
        // temp0 là nhãn, còn lại là feature
        //  temp.length  = 117
        String[] temp = value.toString().split("\\,");

        if (count == 1) {
            //Wi1 -> Wi116
            Wi.add(0.0f);
            for (int i = 1; i < temp.length; i++) {
                Float f = context.getConfiguration().getFloat("W" + i, 0);
                Wi.add(f);
            }

            Xi = new Float[temp.length];
        }
        //1 - 116 
        for (int i = 1; i < Xi.length; i++) {
            Xi[i] = Float.parseFloat(temp[i]);
        }

        float sum = 0;

        for (int i = 1; i < Xi.length; i++) {
            sum += (Xi[i] * Wi.get(i));
        }

        predic = (float) (1 / (1 + (Math.exp(-sum))));

        float Yi = Float.parseFloat(temp[0]);

        for (int i = 1; i < Xi.length; i++) {
            float tmp = Wi.get(i);
            Wi.remove(i);
            Wi.add(i, tmp + lr * (Yi - predic) * (Xi[i]));
        }

        for (int i = 1; i < Wi.size(); i++) {
            try {
                context.write(new Text("W" + i), new FloatWritable(Wi.get(i)));
            } catch (IOException | InterruptedException e) {
            }
        }
    }
}
