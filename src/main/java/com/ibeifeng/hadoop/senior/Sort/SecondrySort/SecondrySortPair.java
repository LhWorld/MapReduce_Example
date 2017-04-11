package com.ibeifeng.hadoop.senior.Sort.SecondrySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建主键类 SecondrySortPair,把第一列整数和第二列整数作为类的属性。
 * @author hadoop
 *
 */
public class SecondrySortPair implements WritableComparable<SecondrySortPair>{
    private static final Logger logger = LoggerFactory.getLogger(SecondrySortMapper.class);
    private int first = 0;
    private int second = 0;
    public void set(int first, int second) {
        logger.info("-------自定义Key和排序阶段set-first----------"+  first+"");
        logger.info("-------自定义Key和排序阶段set-second----------"+ second+"");
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }
    public int getSecond() {
        return second;
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }
    //这里的代码是关键，因为对key排序时，调用的就是这个compareTo方法

    public int compareTo(SecondrySortPair o) {
        logger.info("-------自定义Key和排序阶段compareTo--SecondrySortPair----------"+ o+"");
        if (first != o.first) {
            logger.info("-------自定义Key和排序阶段compareTo--o.first----------"+ o.first+"");
            logger.info("-------自定义Key和排序阶段compareTo--first----------"+ first+"");
            logger.info("-------自定义Key和排序阶段compareTo--(first - o.first)----------"+ (first - o.first)+"");
            return first - o.first;
        }
        else if (second != o.second) {
            logger.info("-------自定义Key和排序阶段compareTo--o.second----------"+ o.second+"");
            logger.info("-------自定义Key和排序阶段compareTo--second----------"+ second+"");
            logger.info("-------自定义Key和排序阶段compareTo--(second - o.second)----------"+ (second - o.second)+"");
            return second - o.second;
        }
        else {
            logger.info("-------自定义Key和排序阶段compareTo--return 0----------"+"");
            return 0;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SecondrySortPair) {
            SecondrySortPair o = (SecondrySortPair) obj;
            logger.info("-------自定义Key和排序阶段equals SecondrySortPair----------"+ o+"");
            logger.info("-------自定义Key和排序阶段equals---o.first----------"+ o.first+"");
            logger.info("-------自定义Key和排序阶段equals--first----------"+ first+"");
            logger.info("-------自定义Key和排序阶段equals--o.second----------"+ o.second+"");
            logger.info("-------自定义Key和排序阶段equals--second----------"+ second+"");
            logger.info("-------自定义Key和排序阶段equals--first == o.first && second == o.second----------"+( first == o.first && second == o.second)+"");
            return first == o.first && second == o.second;
        }
        else {
            logger.info("-------自定义Key和排序阶段equals--false----------");
            return false;
        }
    }

    @Override
    public int hashCode() {
        logger.info("-------自定义Key和排序阶段hasCode-- first----------"+  first+"");
        logger.info("-------自定义Key和排序阶段hasCode--second ----------"+second+"");
        return first + "".hashCode() + second + "".hashCode();
    }
}