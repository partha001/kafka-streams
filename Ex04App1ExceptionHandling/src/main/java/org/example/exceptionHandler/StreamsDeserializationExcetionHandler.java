package org.example.exceptionHandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;


@Slf4j
public class StreamsDeserializationExcetionHandler implements DeserializationExceptionHandler {

    int errorCount =0;
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("exception is : {} and the kafka record is :{}", exception.getMessage(), record,exception);

        //lets say we want to tolerate only 2 exceptions
        log.info("error count: {}",errorCount);
        if(errorCount<2){
            errorCount++;
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
