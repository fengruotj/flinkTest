package com.basic.model;

/**
 * locate com.basic.model
 * Created by 79875 on 2017/4/9.
 *  WordCount 输出模型类
 */
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {}

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
