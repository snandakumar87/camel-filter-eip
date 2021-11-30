// camel-k: language=java

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.BindToRegistry;
import org.apache.camel.PropertyInject;


public class FilterEIP extends RouteBuilder {
    private String consumerMaxPollRecords = "50000";
    private String consumerCount = "3";
    private String consumerSeekTo = "end";
    private String consumerGroup = "consumer-decision-processor";
    private  String kafkaBootstrap = "my-cluster-kafka-brokers:9092";



    @Override
    public void configure() throws Exception {


        rest("/transaction")
                .post()
                .route()
                .removeHeaders("*")
                .convertBodyTo(String.class)
                .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                .setHeader("Content-Type",constant("application/json"))
                .to("http://transaction-risk-filter-eip.apps.cluster-npljc.npljc.sandbox1745.opentlc.com/TransactionRisk")
                .convertBodyTo(String.class)
                .process(new ParseDecisionResult())
                .filter(simple("${header.highRisk} == true"))
                .to("kafka:"+"txn-topic"+ "?brokers=" + kafkaBootstrap);

    }


    private final class ParseDecisionResult implements Processor {



        @Override
        public void process(Exchange exchange) throws Exception {

            java.util.Map valueMap = new com.fasterxml.jackson.databind.ObjectMapper().readValue(exchange.getIn().getBody().toString(), java.util.HashMap.class);

            exchange.getIn().setHeader("highRisk",new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(valueMap.get("risk")));

        }


        }

    }

