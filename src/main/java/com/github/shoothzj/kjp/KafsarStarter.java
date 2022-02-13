/**
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
 */

package com.github.shoothzj.kjp;

import com.github.shoothzj.kjp.config.KafkaConfig;
import com.github.shoothzj.kjp.config.KafsarConfig;
import com.github.shoothzj.kjp.config.PulsarConfig;
import com.github.shoothzj.kjp.config.PulsarConsumeConfig;
import com.github.shoothzj.kjp.config.PulsarProduceConfig;
import com.github.shoothzj.kjp.constant.ConfigConst;
import com.github.shoothzj.kjp.util.EnvUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafsarStarter {

    public static void main(String[] args) {
        log.info("begin to start kafsar broker");
        KafsarConfig kafsarConfig = new KafsarConfig();
        KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.setHost(EnvUtil.getStringVar(ConfigConst.KAFKA_HOST_PROPERTY_NAME, ConfigConst.KAFKA_HOST_ENV_NAME,
                ConfigConst.KAFKA_HOST_DEFAULT_VALUE));
        kafkaConfig.setPort(EnvUtil.getIntVar(ConfigConst.KAFKA_PORT_PROPERTY_NAME, ConfigConst.KAFKA_PORT_ENV_NAME,
                ConfigConst.KAFKA_PORT_DEFAULT_VALUE));
        kafsarConfig.setKafkaConfig(kafkaConfig);
        PulsarConfig pulsarConfig = new PulsarConfig();
        pulsarConfig.setHost(EnvUtil.getStringVar(ConfigConst.PULSAR_HOST_PROPERTY_NAME,
                ConfigConst.PULSAR_HOST_ENV_NAME, ConfigConst.PULSAR_HOST_DEFAULT_VALUE));
        pulsarConfig.setHttpPort(EnvUtil.getIntVar(ConfigConst.PULSAR_HTTP_PORT_PROPERTY_NAME,
                ConfigConst.PULSAR_HTTP_PORT_ENV_NAME, ConfigConst.PULSAR_HTTP_PORT_DEFAULT_VALUE));
        pulsarConfig.setTcpPort(EnvUtil.getIntVar(ConfigConst.PULSAR_TCP_PORT_PROPERTY_NAME,
                ConfigConst.PULSAR_TCP_PORT_ENV_NAME, ConfigConst.PULSAR_TCP_PORT_DEFAULT_VALUE));
        PulsarProduceConfig pulsarProduceConfig = new PulsarProduceConfig();
        pulsarProduceConfig.setDisableBatching(EnvUtil.getBooleanVar(
                ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_PROPERTY_NAME,
                ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_ENV_NAME,
                ConfigConst.PULSAR_PRODUCE_DISABLE_BATCHING_DEFAULT_VALUE));
        pulsarConfig.setProduceConfig(pulsarProduceConfig);
        PulsarConsumeConfig pulsarConsumeConfig = new PulsarConsumeConfig();
        pulsarConsumeConfig.setReceiverQueueSize(EnvUtil.getIntVar(
                ConfigConst.PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_PROPERTY_NAME,
                ConfigConst.PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_ENV_NAME,
                ConfigConst.PULSAR_CONSUME_RECEIVER_QUEUE_SIZE_DEFAULT_VALUE
        ));
        pulsarConfig.setConsumeConfig(pulsarConsumeConfig);
        KafsarBroker kafsarBroker = new KafsarBroker(kafsarConfig);
        kafsarBroker.start();
    }

}
