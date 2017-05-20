package com.basic.bolt;

import com.basic.model.WordWithCount;
import com.basic.task.WordCountTupleTask;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * locate com.basic.bolt
 * Created by 79875 on 2017/4/14.
 */
public class WordCountReduceBolt extends RichReduceFunction<WordWithCount> {
    private static Timer timer;
    private static long tupplecount=0; //记录单位时间通过的元组数量
    private static boolean m_bool=true;//判断使计时器合理运行
    private static long alltupplecount=0;//记录所有通过本Test的元组数量
    private static final Logger LOG = LoggerFactory.getLogger(WordCountReduceBolt.class);
    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 200, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(100));
    static {
        timer=new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(!m_bool) {
                    executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),tupplecount));
                    //resultQueue.add(new WordCountResult(System.currentTimeMillis(),tupplecount));
                    tupplecount = 0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒

    }
    @Override
    public void open(Configuration parameters) throws Exception {
        m_bool=false;//让计时器运行
        LOG.info("------------prepare------------");
    }

    @Override
    public WordWithCount reduce(WordWithCount a, WordWithCount b) throws Exception {
        tupplecount++;
        alltupplecount++;
        return new WordWithCount(a.word, a.count + b.count);
    }
}
