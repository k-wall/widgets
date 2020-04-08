/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package widgets;


import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.IllegalStateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class HelloWorld {

    public static final String FABRICATE_REQUEST = "fabricate-requests";
    public static final String TEMPERATURE = "plant-temperature";
    public static final String PRESSURE = "plant-pressure";

    public static void main(String[] args) throws Exception {

        HelloWorld helloWorld = new HelloWorld();

        // Queue-space
        // Each producer max 1 per second, consumer 0.75 per second.
        helloWorld.makeTraffic("fab1", "fab", Arrays.asList(FABRICATE_REQUEST, FABRICATE_REQUEST),
                Arrays.asList(FABRICATE_REQUEST, FABRICATE_REQUEST), 10, 7.55);

        helloWorld.makeTraffic("fab2", "fab", Arrays.asList(FABRICATE_REQUEST),
                Arrays.asList(FABRICATE_REQUEST, FABRICATE_REQUEST, FABRICATE_REQUEST, FABRICATE_REQUEST), 10, 8.5);


        helloWorld.makeTraffic("tpub", "processing", Arrays.asList(TEMPERATURE),
                Collections.emptyList(), 50, 0  );

        helloWorld.makeTraffic("tcon1", "processing", Collections.emptyList(),
                Arrays.asList(TEMPERATURE), 1, 50  );

        helloWorld.makeTraffic("tcon2", "processing", Collections.emptyList(),
                Arrays.asList(TEMPERATURE), 1, 50  );


        helloWorld.makeTraffic("pub", "processing", Arrays.asList(PRESSURE),
                Collections.emptyList(), 100, 5);
        helloWorld.makeTraffic("pcon1", "processing", Collections.emptyList(),
                Arrays.asList(PRESSURE, PRESSURE), 1, 100);
        helloWorld.makeTraffic("pcon2", "processing", Arrays.asList(PRESSURE),
                Arrays.asList(PRESSURE, PRESSURE), 1, 100);


    }


    private void makeTraffic(String connectionName, String conLookup, List<String> producerLookups, List<String> consumerLookups, int initialProduceRate, double initialConsumeRate) throws Exception {

        Context context = new InitialContext();

        ConnectionFactory factory = (ConnectionFactory) context.lookup(conLookup);

        Connection connection = factory.createConnection();
        connection.setExceptionListener(new MyExceptionListener());
        connection.start();

        List<Destination> producerDests = producerLookups.stream().map(d -> {
            try {
                return (Destination)context.lookup(d);
            } catch (NamingException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());

        List<Destination> consumerDests = consumerLookups.stream().map(d -> {
            try {
                return (Destination)context.lookup(d);
            } catch (NamingException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toList());


        AtomicInteger producerNum = new AtomicInteger();
        AtomicDouble produceRate = new AtomicDouble(initialProduceRate);
        producerDests.forEach(d -> {

            String producerName = String.format("producer-%d", producerNum.get());
            final double rate = produceRate.get();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    producerForever(connection, d, rate, producerName, connectionName);

                }
            }).start();
            produceRate.set(rate * 5);
            producerNum.getAndIncrement();
        });

        AtomicInteger consumerNum = new AtomicInteger();
        AtomicDouble consumerRate = new AtomicDouble(initialConsumeRate);
        consumerDests.forEach(d -> {

            String consumerName = String.format("consumer-%d", consumerNum.get());
            final double rate = consumerRate.get();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    consumeForever(connection, d, rate, consumerName, connectionName);

                }
            }).start();
            consumerRate.set(rate * 5);
            consumerNum.getAndIncrement();
        });
    }

    private void producerForever(Connection connection, Destination d, double produceRate, String producerName, String name) {
        try {
            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = producerSession.createProducer(d);

            RateLimiter rateLimiter = RateLimiter.create(produceRate);

            int count=0;
            long batchStart = System.currentTimeMillis();
            while(true) {
                rateLimiter.acquire();
                producer.send(producerSession.createMessage());
                count++;
                if (count % 100 == 0) {
                    long took = System.currentTimeMillis() - batchStart;
                    System.out.printf("%s/%s took %d ms to produce %d messages\n", name, producerName, took/1000, count);
                    count = 0;
                    batchStart = System.currentTimeMillis();
                }

            }


        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    private void consumeForever(Connection connection, Destination d, double consumeRate,  String consumerName, String name) {
        try {
            Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer = consumerSession.createConsumer(d);

            RateLimiter rateLimiter = RateLimiter.create(consumeRate);

            int count=0;
            long batchStart = System.currentTimeMillis();
            while(true) {
                rateLimiter.acquire();
                if (consumer.receive(1000) != null) {
                    count++;
                    if (count % 100 == 0) {
                        long took = System.currentTimeMillis() - batchStart;
                        System.out.printf("%s/%s took %d ms to consume %d messages\n", name, consumerName, took, count);
                        count = 0;
                        batchStart = System.currentTimeMillis();
                    }
                }


            }


        } catch (JMSException e) {
            throw new IllegalStateException(e);
        }
    }

    private static class MyExceptionListener implements ExceptionListener {
        @Override
        public void onException(JMSException exception) {
            System.out.println("Connection ExceptionListener fired, exiting.");
            exception.printStackTrace(System.out);
            System.exit(1);
        }
    }
}
