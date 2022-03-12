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

import com.github.shoothzj.kjp.config.KafsarConfig;
import com.github.shoothzj.kjp.util.EventLoopUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;

public class KafsarBroker {

    private final KafsarConfig kafsarConfig;

    private final EventLoopGroup acceptorGroup;

    private final EventLoopGroup workerGroup;

    public KafsarBroker(KafsarConfig kafsarConfig) {
        this.kafsarConfig = kafsarConfig;
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("kafka-acceptor"));
        this.workerGroup = EventLoopUtil.newEventLoopGroup(1, new DefaultThreadFactory("kafka-worker"));
    }

    public void start() {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        serverBootstrap.childHandler(new KafkaChannelInitializer());
        serverBootstrap.bind(kafsarConfig.getKafkaConfig().getHost(), kafsarConfig.getKafkaConfig().getPort());
    }

    public String getKafkaHost() {
        return kafsarConfig.getKafkaConfig().getHost();
    }

    public int getKafkaPort() {
        return kafsarConfig.getKafkaConfig().getPort();
    }

    public String getPulsarHost() {
        return kafsarConfig.getPulsarConfig().getHost();
    }

    public int getPulsarHttpPort() {
        return kafsarConfig.getPulsarConfig().getHttpPort();
    }

    public int getPulsarTcpPort() {
        return kafsarConfig.getPulsarConfig().getTcpPort();
    }

}
