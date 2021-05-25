/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        int index = cidAll.indexOf(currentCID);
        int mod = mqAll.size() % cidAll.size();
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }


    public static void main(String[] args) {
        AllocateMessageQueueAveragely allocateMessageQueueAveragely = new AllocateMessageQueueAveragely();
        List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName("b1");
        messageQueue.setTopic("t1");
        messageQueue.setQueueId(1);
        mqAll.add(messageQueue);

        messageQueue = new MessageQueue();
        messageQueue.setBrokerName("b1");
        messageQueue.setTopic("t1");
        messageQueue.setQueueId(2);
        mqAll.add(messageQueue);


        messageQueue = new MessageQueue();
        messageQueue.setBrokerName("b1");
        messageQueue.setTopic("t1");
        messageQueue.setQueueId(3);
        mqAll.add(messageQueue);

        messageQueue = new MessageQueue();
        messageQueue.setBrokerName("b1");
        messageQueue.setTopic("t1");
        messageQueue.setQueueId(4);
        mqAll.add(messageQueue);


        List<String> list = new ArrayList<String>();
        list.addAll(Arrays.asList("m1", "m2", "m3"));
        List<MessageQueue> messageQueues = allocateMessageQueueAveragely.allocate("g1", "m1", mqAll, list);

        System.out.println(messageQueues);
        // 连续分配的方式
        //[MessageQueue [topic=t1, brokerName=b1, queueId=1], MessageQueue [topic=t1, brokerName=b1, queueId=2]]
    }
}
