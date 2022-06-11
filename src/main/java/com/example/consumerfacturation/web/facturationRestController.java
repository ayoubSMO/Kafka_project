package com.example.consumerfacturation.web;

import com.example.consumerfacturation.entities.Facturation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class facturationRestController {

    @Autowired
    private StreamBridge streamBridge ;
    @Autowired
    private InteractiveQueryService interactiveQueryService ;
    @GetMapping("/publish/{topic}/{nameClt}")
    public Facturation publish(@PathVariable String topic , @PathVariable String nameClt){
        Facturation facturation = new Facturation(new Random().nextInt(9000) ,"Client" + new Random().nextInt(9000) , new Random().nextDouble()*1000);
        streamBridge.send(topic , facturation);
        return facturation;
    }

    @GetMapping(path = "/analytics/{client}" ,produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String ,Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> {
                    Map<String ,Long> stringLongMap = new HashMap<>();
                    ReadOnlyWindowStore<String ,Long> windowStore = interactiveQueryService.getQueryableStore("page-count" , QueryableStoreTypes.windowStore());
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000) ;
                    KeyValueIterator<Windowed<String>,Long> fetchAll = windowStore.fetchAll(from ,now) ;
                    //WindowStoreIterator<Long> fetchAll = windowStore.fetch(page ,from ,now) ;
                    while (fetchAll.hasNext()){
                        KeyValue<Windowed<String> ,Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key() ,next.value);
                    }
                    return stringLongMap;
                }).share(); // Ksql
    }
}
