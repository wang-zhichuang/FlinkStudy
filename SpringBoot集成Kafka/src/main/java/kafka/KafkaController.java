package kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {
    @Autowired
    KafkaService kafkaService;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @RequestMapping("/send")
    public void send() {
        kafkaService.sendMessage("test","testettt");
        System.out.println("send");
    }

    @RequestMapping("newKafka")
    public void newKafka(){

    }
    // 开启监听
    @RequestMapping("/start")
    public void start() {
        System.out.println("开启监听");
        //判断监听容器是否启动，未启动则将其启动
        if (!registry.getListenerContainer("myListener1").isRunning()) {
            registry.getListenerContainer("myListener1").start();
        }
        registry.getListenerContainer("myListener1").resume();
    }

    // 关闭监听
    @RequestMapping("/stop")
    public void stop() {
        System.out.println("关闭监听");
        //判断监听容器是否启动，未启动则将其启动
        registry.getListenerContainer("myListener1").pause();
    }
}
