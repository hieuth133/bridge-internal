package vn.vatm.bridge;

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

        CachingConnectionFactory cf = new CachingConnectionFactory(
                prop("rabbitmq.host"), Integer.parseInt(prop("rabbitmq.port")));
        cf.setVirtualHost(prop("rabbitmq.vhost"));
        cf.setUsername(prop("rabbitmq.username"));
        cf.setPassword(prop("rabbitmq.password"));

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
    }

    private String prop(String key) throws Exception {
        return getContext().resolvePropertyPlaceholders("{{" + key + "}}");
    }
}
