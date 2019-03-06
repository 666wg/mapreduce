package cn.xjw.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//        Java类型   <--->   Hadoop Writable类型
//        boolean            BooleanWritable
//        byte               ByteWritable
//        int                IntWritable
//        float	             FloatWritable
//        long	             LongWritable
//        double	         DoubleWritable
//        string	         Text
//        map	             MapWritable
//        array	             ArrayWritable

/**
 * 驱动主程序
 */
public class WordCountDriver {

	public static void main(String[] args) throws Exception {
        // 1 获取配置信息，或者job对象实例
        Configuration configuration = new Configuration();
        // 即使没有下面这行,也可以本地运行。因\hadoop-mapreduce-client-core.jar!\mapred-default.xml 中默认的参数就是 local
        configuration.set("mapreduce.framework.name","local");

        Job job = Job.getInstance(configuration);

        // 2 指定本次mr job jar包运行主类
        job.setJarByClass(WordCountDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 指定job的输入原始文件所在目录，输入目录下可放多个文件，eg：data1.txt  data2.txt ......
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\input"));
        // 7 指定文件输出目录，注意：输出目录output在程序执行之前并不存在，存在则报错
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
	}

}
