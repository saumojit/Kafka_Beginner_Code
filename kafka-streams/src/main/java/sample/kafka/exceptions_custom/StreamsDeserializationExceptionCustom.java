package sample.kafka.exceptions_custom;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sample.kafka.StringUpperConvertExample;

import java.util.Map;

public class StreamsDeserializationExceptionCustom implements DeserializationExceptionHandler {
    int error_count=0;
    Logger log= LoggerFactory.getLogger(StringUpperConvertExample.class.getSimpleName());

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> record, Exception e) {
        error_count++;
        log.info("Error Occured while Deserialization (Topic to Streams) -- " +
                        "Error Count = {} , Exception = {} , For Record = {} "
                        , error_count ,e.getMessage() , record) ;
        if(error_count>4){
            log.info("Process Exiting With Response as FAIL");
            return DeserializationHandlerResponse.FAIL;
        }
        log.info("Process Continuing With Response as CONTINUE");
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
