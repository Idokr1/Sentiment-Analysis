package com.handson.sentiment.controller;

import com.handson.sentiment.nlp.SentimentAnalyzer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
public class AppController {
    @Autowired
    SentimentAnalyzer sentimentAnalyzer;

    @RequestMapping(path = "/hello", method = RequestMethod.GET)
    public  @ResponseBody Mono<String> hello(String text)  {
        Double score =  sentimentAnalyzer.analyze(text);
        return Mono.just("Score is:" + score);
    }
}
