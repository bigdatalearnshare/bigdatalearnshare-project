package tech.bigdatalearnshare.recommendation.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Component;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author bigdatalearnshare
 * @Date 2018-07-11
 */
@Component
public class ProducerJ {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Autowired
    private JmsTemplate jmsTemplate;

    @Autowired
    private Destination destination;

    public void work() throws Exception {
        //MQ发送消息给spark同步更新院校推荐程序
        this.sendMQ("add", 119);
//        this.sendMQ("update", 119);
//        this.sendMQ("delete", 119);
    }

    private void sendMQ(String type, int sid) {
        this.jmsTemplate.send(this.destination, new MessageCreator() {
            @Override
            public Message createMessage(Session session) throws JMSException {
                //创建消息体TextMessage
                TextMessage textMessage = new ActiveMQTextMessage();

                //拼接消息内容，可以发送json数据
                //使用Map封装数据
                Map<String, Object> map = new HashMap<>();
                map.put("type", type);
                map.put("sid", sid);

                try {
                    //将Map转为json格式数据
                    String json = MAPPER.writeValueAsString(map);
                    //设置json格式的消息到消息体中
                    textMessage.setText(json);
                    // 消息发送成功，打印
                    System.out.println("消息发送成功,内容是：" + json);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return textMessage;
            }
        });
    }
}
