package cn.xjw.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class WordCountTest {

    /**
     * @Author: w
     * @Description: 测试Mapper
     */
    @Test
    public void WordCountMapperTest() throws IOException {
        /** 构造输入值 */
        Pair<LongWritable, Text> line1 = new Pair<LongWritable, Text>(new LongWritable(0), new Text("zhangsan lisi wangwu"));
        Pair<LongWritable, Text> line2 = new Pair<LongWritable, Text>(new LongWritable(0), new Text("zhangsan lisi"));
        List<Pair<LongWritable, Text>> inList = new ArrayList<Pair<LongWritable, Text>>();
        inList.add(line1);
        inList.add(line2);

        /** 构造输出值 */
        Pair<Text, IntWritable> outputRecord1 = new Pair<Text, IntWritable>(new Text("zhangsan"), new IntWritable(1));
        Pair<Text, IntWritable> outputRecord2 = new Pair<Text, IntWritable>(new Text("lisi"), new IntWritable(1));
        Pair<Text, IntWritable> outputRecord3 = new Pair<Text, IntWritable>(new Text("wangwu"), new IntWritable(1));
        Pair<Text, IntWritable> outputRecord4 = new Pair<Text, IntWritable>(new Text("zhangsan"), new IntWritable(1));
        Pair<Text, IntWritable> outputRecord5 = new Pair<Text, IntWritable>(new Text("lisi"), new IntWritable(1));
        List<Pair<Text, IntWritable>> list = new ArrayList<Pair<Text, IntWritable>>();
        /* 特别注意:*/
        //（本人亲测）需要注意输出值顺序，否则测试不通过
        list.add(outputRecord1);
        list.add(outputRecord2);
        list.add(outputRecord3);
        list.add(outputRecord4);
        list.add(outputRecord5);

        new MapDriver<LongWritable, Text, Text, IntWritable>()
                // 配置mapper
                .withMapper(new WordCountMapper())
                //单行输入
//                .withInput(new LongWritable(0), new Text("zhangsan lisi wangwu"))
                //多行输入
                .withAll(inList)
                //单行输出
//                .withOutput(new Text("zhangsan"), new IntWritable(1))
                //多行输出
                .withAllOutput(list)
                .runTest();
    }


    /**
     * @Author: w
     * @Description: 测试Reducer
     */
    @Test
    public void WordCountReducerTest() throws IOException {
        /** 构造输入值 */
        List<IntWritable> zhangsanList = new ArrayList<IntWritable>();
        zhangsanList.add(new IntWritable(1));
        zhangsanList.add(new IntWritable(1));
        List<IntWritable> lishiList = new ArrayList<IntWritable>();
        lishiList.add(new IntWritable(1));
        lishiList.add(new IntWritable(1));
        List<IntWritable> wangwuList = new ArrayList<IntWritable>();
        wangwuList.add(new IntWritable(1));
        Pair<Text, List<IntWritable>> line1 = new Pair<Text, List<IntWritable>>(new Text("zhangsan"), zhangsanList);
        Pair<Text, List<IntWritable>> line2 = new Pair<Text, List<IntWritable>>(new Text("lisi"), lishiList);
        Pair<Text, List<IntWritable>> line3 = new Pair<Text, List<IntWritable>>(new Text("wangwu"), wangwuList);
        List<Pair<Text, List<IntWritable>>> inList = new ArrayList<Pair<Text, List<IntWritable>>>();
        inList.add(line1);
        inList.add(line2);
        inList.add(line3);

        /** 构造输出值 */
        Pair<Text, IntWritable> outputRecord1 = new Pair<Text, IntWritable>(new Text("zhangsan"), new IntWritable(2));
        Pair<Text, IntWritable> outputRecord2 = new Pair<Text, IntWritable>(new Text("lisi"), new IntWritable(2));
        Pair<Text, IntWritable> outputRecord3 = new Pair<Text, IntWritable>(new Text("wangwu"), new IntWritable(1));
        List<Pair<Text, IntWritable>> list = new ArrayList<Pair<Text, IntWritable>>();
        /* 特别注意:*/
        //（本人亲测）需要注意输出值顺序，否则测试不通过
        list.add(outputRecord1);
        list.add(outputRecord2);
        list.add(outputRecord3);

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                //设置Reducer
                .withReducer(new WordCountReducer())
                //设置输入key和List
                .withAll(inList)
                //设置期望输出
                .withAllOutput(list)
                //运行测试
                .runTest();
    }


    /**
     * @Author: w
     * @Description: 测试Driver
     */
    @Test
    public void WordCountDriverTest() throws IOException {
        /** 构造输入值 */
        Pair<LongWritable, Text> line1 = new Pair<LongWritable, Text>(new LongWritable(0), new Text("zhangsan lisi wangwu"));
        Pair<LongWritable, Text> line2 = new Pair<LongWritable, Text>(new LongWritable(0), new Text("zhangsan lisi"));
        List<Pair<LongWritable, Text>> inList = new ArrayList<Pair<LongWritable, Text>>();
        inList.add(line1);
        inList.add(line2);

        /** 构造输出值 */
        Pair<Text, IntWritable> outputRecord1 = new Pair<Text, IntWritable>(new Text("zhangsan"), new IntWritable(2));
        Pair<Text, IntWritable> outputRecord2 = new Pair<Text, IntWritable>(new Text("lisi"), new IntWritable(2));
        Pair<Text, IntWritable> outputRecord3 = new Pair<Text, IntWritable>(new Text("wangwu"), new IntWritable(1));
        List<Pair<Text, IntWritable>> list = new LinkedList<Pair<Text, IntWritable>>();
        /* 特别注意:*/
        //（本人亲测）需要注意输出值顺序，否则测试不通过
        list.add(outputRecord2);
        list.add(outputRecord3);
        list.add(outputRecord1);

        new MapReduceDriver<LongWritable,Text,Text, IntWritable, Text, IntWritable>()
                //设置Mapper
                .withMapper(new WordCountMapper())
                //设置Reducer
                .withReducer(new WordCountReducer())
                //设置输入key和List
                .withAll(inList)
                //设置期望输出
                .withAllOutput(list)
                //运行测试
                .runTest();
    }


}
