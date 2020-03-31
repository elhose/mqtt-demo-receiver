package com.ttpsc.mqttdemo;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

@SpringBootApplication
public class MqttDemoApplication {

    private final String HOST;
    private final Integer PORT;
    private final String CLIENT_ID;
    private final String TOPIC;
    private final Integer COMPLETION_TIMEOUT;
    private final Integer QOS;

    public MqttDemoApplication(@Value("${mqtt.hostname}") String host, @Value("${mqtt.port}") Integer port, @Value("${mqtt.clientId}") String clientId,
                               @Value("${mqtt.topic}") String topic,@Value("${mqtt.completionTimeout}") Integer completion_timeout,@Value("${mqtt.qos}") Integer qos) {
        this.HOST = host;
        this.PORT = port;
        this.CLIENT_ID = clientId;
        this.TOPIC = topic;
        this.COMPLETION_TIMEOUT = completion_timeout;
        this.QOS = qos;
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(MqttDemoApplication.class)
                .run(args);
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageProducer inbound() {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter("tcp://" + HOST + ":" + PORT, CLIENT_ID,
                        TOPIC);
        adapter.setCompletionTimeout(COMPLETION_TIMEOUT);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(QOS);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        return new MessageHandler() {

            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                System.out.println(message.getPayload());
            }
        };
    }
}
