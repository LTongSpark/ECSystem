package com.ml;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
/**
 * Project: ECSystem
 * ClassName: com.ml.LogProcessor
 * Version: V1.0
 * Author: LTong
 * Date: 2019-06-24 上午 10:00
 */
public class LogProcessor implements Processor<byte[], byte[]>{
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        // 核心处理流程
        String input = new String(line);
        // 提取数据，以固定前缀过滤日志信息
        if( input.contains("PRODUCT_RATING_PREFIX:") ){
            System.out.println("product rating data coming! " + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}

