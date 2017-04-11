package com.ibeifeng.hadoop.senior.Sort.SecondrySort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在分组比较时，只比较原来的key,而不是组合key
 * @author hadoop
 *
 */
public class GroupingComparator implements RawComparator<SecondrySortPair>{
    private static final Logger logger = LoggerFactory.getLogger(GroupingComparator.class);
    public int compare(SecondrySortPair o1, SecondrySortPair o2) {

        int first1 = o1.getFirst();
        int first2 = o2.getFirst();
        logger.info("-------group阶段first1----------"+  first1+"");
        logger.info("-------group阶段first2----------"+ first2+"");
        logger.info("-------group阶段first1减去first2----------"+ (first1 - first2)+"");
        return first1 - first2;

    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        logger.info("-------group阶段b1----------"+  b1.toString()+"");
        logger.info("-------group阶段s1----------"+ s1+"");
        logger.info("-------group阶段l1----------"+ l1+"");
        logger.info("-------group阶段b2----------"+  b2.toString()+"");
        logger.info("-------group阶段s2----------"+ s2+"");
        logger.info("-------group阶段l2----------"+ l2+"");
        logger.info("-------group阶段返回值----------"+ WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8)+"");
        return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
    }
}