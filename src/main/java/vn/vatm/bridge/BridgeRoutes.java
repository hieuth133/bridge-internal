package vn.vatm.bridge;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.springrabbit.SpringRabbitMQComponent;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

public class BridgeRoutes extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        List<BridgeRule> rules = BridgeRule.parse(getContext().resolvePropertyPlaceholders("{{bridge.rules}}"));

        errorHandler(defaultErrorHandler().maximumRedeliveries(-1).redeliveryDelay(1000)
                .allowRedeliveryWhileStopping(false));

        CachingConnectionFactory cf = new CachingConnectionFactory(
                prop("rabbitmq.host"), Integer.parseInt(prop("rabbitmq.port")));
        cf.setVirtualHost(prop("rabbitmq.vhost"));
        cf.setUsername(prop("rabbitmq.username"));
        cf.setPassword(prop("rabbitmq.password"));
        cf.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);

        RabbitAdmin admin = new RabbitAdmin(cf);
        admin.declareExchange(new TopicExchange(prop("bridge.out.exchange")));
        TopicExchange inEx = new TopicExchange(prop("bridge.in.exchange"));
        admin.declareExchange(inEx);
        Queue q = new Queue(prop("bridge.in.queue"));
        admin.declareQueue(q);
        admin.declareBinding(BindingBuilder.bind(q).to(inEx).with("#"));
        admin.declareQueue(new Queue(prop("bridge.in.dlq")));

        SpringRabbitMQComponent rabbit = new SpringRabbitMQComponent();
        rabbit.setConnectionFactory(cf);
        getContext().addComponent("spring-rabbitmq", rabbit);

        com.solacesystems.jms.SolConnectionFactory sol = com.solacesystems.jms.SolJmsUtility.createConnectionFactory();
        sol.setHost(prop("solace.host"));
        sol.setVPN(prop("solace.vpn"));
        sol.setUsername(prop("solace.username"));
        sol.setPassword(prop("solace.password"));
        sol.setDynamicDurables(true);
        sol.setDirectTransport(false);
        var jmsCf = new org.springframework.jms.connection.CachingConnectionFactory(sol);
        jmsCf.setCacheConsumers(false);
        getContext().addComponent("jms", org.apache.camel.component.jms.JmsComponent.jmsComponent(jmsCf));

        // Services are stopped after the routes, so the connections close only once no exchange is in flight.
        getContext().addService(new org.apache.camel.support.service.ServiceSupport() {
            @Override
            protected void doStop() {
                cf.destroy();
                jmsCf.destroy();
            }
        });

        for (BridgeRule r : rules) {
            if (r.direction() != BridgeRule.Direction.OUT) {
                continue;
            }
            from("jms:topic:" + r.source() + ">?subscriptionDurable=true&durableSubscriptionName={{bridge.out.durable-prefix}}"
                    + r.name() + "&disableReplyTo=true&acknowledgementModeName=CLIENT_ACKNOWLEDGE")
                    .routeId("out-" + r.name())
                    .filter(header("bridgeOrigin").isNull())
                    .process(e -> e.getMessage().setHeader("CamelSpringRabbitmqRoutingOverrideKey",
                            r.map(e.getMessage().getHeader("JMSDestination", jakarta.jms.Topic.class).getTopicName())))
                    .setHeader("CamelSpringRabbitmqMessageId", header("JMSMessageID"))
                    .setHeader("CamelSpringRabbitmqCorrelationId", header("JMSCorrelationID"))
                    .setHeader("CamelSpringRabbitmqContentType", header("contentType"))
                    .removeHeaders("JMS*")
                    .setHeader("bridgeOrigin", constant("solace"))
                    .to("spring-rabbitmq:{{bridge.out.exchange}}");
        }

        from("spring-rabbitmq:{{bridge.in.exchange}}?queues={{bridge.in.queue}}&autoDeclare=false&disableReplyTo=true&rejectAndDontRequeue=false")
                .routeId("in")
                .filter(header("bridgeOrigin").isNull())
                .process(e -> {
                    String key = e.getMessage().getHeader("CamelSpringRabbitmqRoutingKey", String.class);
                    String target = BridgeRule.map(rules, BridgeRule.Direction.IN, key);
                    e.getMessage().setHeader("bridgeRoutingKey", key);
                    if (target != null) {
                        e.getMessage().setHeader("CamelJmsDestinationName", target);
                    }
                })
                .choice()
                    .when(header("CamelJmsDestinationName").isNull())
                        .to("spring-rabbitmq:default?routingKey={{bridge.in.dlq}}")
                    .otherwise()
                        .removeHeader("bridgeRoutingKey")
                        .setHeader("JMSCorrelationID", header("CamelSpringRabbitmqCorrelationId"))
                        .setHeader("contentType", header("CamelSpringRabbitmqContentType"))
                        .setHeader("messageId", header("CamelSpringRabbitmqMessageId"))
                        .process(e -> {
                            String ct = e.getMessage().getHeader("contentType", String.class);
                            Object b = e.getMessage().getBody();
                            if (ct != null && b instanceof byte[] bytes
                                    && (ct.startsWith("application/json") || ct.startsWith("application/xml"))) {
                                e.getMessage().setBody(new String(bytes, StandardCharsets.UTF_8));
                            }
                        })
                        .setHeader("bridgeOrigin", constant("rabbitmq"))
                        .to("jms:topic:bridge-inbound")
                .end();
    }

    private String prop(String key) throws Exception {
        return getContext().resolvePropertyPlaceholders("{{" + key + "}}");
    }
}
