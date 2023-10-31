package com.handson.sentiment.controller;

import com.handson.sentiment.kafka.AppKafkaSender;
import com.handson.sentiment.nlp.SentimentAnalyzer;
import com.handson.sentiment.twitter.AppNewsStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.util.ArrayList;

import static com.handson.sentiment.kafka.KafkaTopicConfig.APP_TOPIC;

@RestController
public class AppController {
    @Autowired
    SentimentAnalyzer sentimentAnalyzer;

    @Autowired
    AppNewsStream twitterStream;

    @Autowired
    AppKafkaSender kafkaSender;

    @Autowired
    KafkaReceiver<String,String> kafkaReceiver;


    @RequestMapping(path = "/sendKafka", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> sendText(String text)  {
        kafkaSender.send(text, APP_TOPIC);
        return Mono.just("OK");
    }

    @RequestMapping(path = "/getKafka", method = RequestMethod.GET)
    public  @ResponseBody  Flux<String> getKafka()  {
        return kafkaReceiver.receive().map(x-> x.value() + "<br>");
    }

    @RequestMapping(path = "/startTwitter", method = RequestMethod.GET)
    public  @ResponseBody Flux<String> start(String text) throws Exception {
        return twitterStream.filter(text)
                .window(Duration.ofSeconds(3))
                .flatMap(window->toArrayList(window))
                .map(messages->{
                    if (messages.size() == 0) return "size: 0 <br>";
                    return "size: " + messages.size() + "<br>";
                });
    }

    @RequestMapping(path = "/stopTwitter", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> stop()  {
        twitterStream.shutdown();
        return Mono.just("shutdown");
    }

    public static <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return  source.reduce(new ArrayList(), (a, b) -> { a.add(b);return a; });
    }

    @RequestMapping(path = "/hello", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> hello(String text)  {
        Double score =  sentimentAnalyzer.analyze(text);
        return Mono.just("Score is:" + score);
    }
}
