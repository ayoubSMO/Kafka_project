package com.example.consumerfacturation.services;

import com.example.consumerfacturation.Repositories.facturationRepository;
import com.example.consumerfacturation.entities.Facturation;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.data.rest.core.config.RepositoryRestConfiguration;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class FacturationService {
    @Autowired
    private facturationRepository facturationRepository;

    @Bean
    public Consumer<Facturation> facturationConsumer() {
        return (input) -> {
            System.out.println("-------------------------");
            System.out.println(input.toString());
            System.out.println("-------------------------");

            List data = new ArrayList();
            FileWriter file = null ;
            data.add(new Facturation(input.getNumFacturation() ,input.getNomClient() ,input.getMontantFacturation())) ;

            try {
                file = new FileWriter("Facturation.csv" ,true) ;
                file.append("\n") ;
            } catch (IOException e) {
                e.printStackTrace();
            }

            Iterator it = data.iterator();
            while (it.hasNext()) {
                Facturation f = (Facturation) it.next();
                try {
                    file.append(String.valueOf(f.getNumFacturation()));
                    file.append(",");
                    file.append(f.getNomClient());
                    file.append(",");
                    file.append(String.valueOf(f.getMontantFacturation()));
                    file.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }


            }

            facturationRepository.save(new Facturation(input.getNumFacturation(), input.getNomClient(), input.getMontantFacturation()));

        };
    }

    @Bean
    public Supplier<Facturation> facturationSupplier() {
        return () ->
                new Facturation(
                        new Random().nextInt(9000),
                        Math.random() > 0.5 ? "clt1" : "clt2",
                        new Random().nextDouble()*999
                );
    }



    @Bean
    public Function<Facturation,Facturation> FacturationService_poller(){
        return (input) -> {
            input.setNumFacturation(new Random().nextInt(9000));
            input.setNomClient("Client" + new Random().nextInt(9000));
            input.setMontantFacturation(new Random().nextDouble() * 1000);
            return input;
        };
    }

    @Bean
    public Function<KStream<String ,Facturation>,KStream<String ,Long>> kStreamFunction(){
        return (input) -> {
            return input
                    .filter((k ,v) -> v.getNumFacturation() > 1)
                    .map((k ,v) -> new KeyValue<>(v.getNomClient() ,0.0))
                    .groupBy((k ,v) -> k, Grouped.with(Serdes.String(),Serdes.Double()))
                    .windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
                    .count(Materialized.as("page-count"))
                    .toStream()
                    .map((k ,v) -> new KeyValue<>("=>" + k.window().startTime() + k.window().endTime() + k.key() ,v));
        };
    }
}
