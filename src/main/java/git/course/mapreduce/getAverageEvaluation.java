package git.course.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class getAverageEvaluation extends Configured implements Tool{
    public static void main( String[] args ) throws Exception {
        int res = ToolRunner.run(new getAverageEvaluation(), args);
        System.exit(res);
    }

    private static Logger logger  =  Logger.getLogger(getAverageEvaluation.class.getName());

    // 配置作业的主要参数和流程
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        ////////////////////////////////////////////////////////////
        // 创建作业，配置作业所需参数
        Configuration conf = new Configuration();
        // 创建作业
        Job job = Job.getInstance(conf, "getAverageEvaluation");
        logger.info("args1=====" + args[0]);
        logger.info("args2=====" + args[1]);

        // 注入作业的主类
        job.setJarByClass(getAverageEvaluation.class);

        // 为作业注入Map和Reduce类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // 指定输入类型为：文本格式文件；注入文本输入格式类
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));     //HDFS路径
        //TextInputFormat.addInputPath(job, new Path("/mapred_input1"));

        // 指定输出格式为：文本格式文件；注入文本输入格式类
        job.setOutputFormatClass(TextOutputFormat.class);
        // 指定Key为文本格式；注入文本类
        job.setOutputKeyClass(Text.class);
        // 执行Value为整型格式；注入整型类
        job.setOutputValueClass(FloatWritable.class);
        // 指定作业的输出目录
        TextOutputFormat.setOutputPath(job, new Path(args[1]));     //HDFS路径
        //TextOutputFormat.setOutputPath(job, new Path("/mapred_output1"));

        ////////////////////////////////////////////////////////////
        // 作业的执行流程
        // 执行MapReduce
        boolean res = job.waitForCompletion(true);
        if(res)
            return 0;
        else
            return -1;
    }

    // Map过程
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();  //将每行数据转化为字符串
            System.out.println("file: " + ((FileSplit)context.getInputSplit()).getPath().toString());
            System.out.println("line: " + key.toString() + "================" + line); //输出日志 key 行

            // 制表符分割数据
            String[] words = line.split("\\t");
            // 下标为 4 表示城市
            Text city = new Text(words[4]);
            FloatWritable score = new FloatWritable();
            // 下标为 5 表示评级分数, 如果是Unrated,则为0
            if (words[5] != null && "Unrated".equals(words[5])) {
                score.set(0);
            } else {
                assert words[5] != null;
                score.set(Float.parseFloat(words[5]));
            }
            context.write(city,score);
        }
    }

    // Reduce过程
    public static class Reduce extends Reducer<Text, FloatWritable, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<FloatWritable> val, Context context)	//mothod for each key，input format key(value1,value2,......)
                throws IOException, InterruptedException {
            float sum = 0;
            //float aver = 0;
            int count = 0;
            for (FloatWritable floatWritable : val) {
                sum += floatWritable.get();
                count++;
            }
            String result = String.format("%.2f", sum / count);
            logger.info(key.toString() + ": " + result);
            context.write(key, new Text(String.valueOf(result)));
        }
    }
}