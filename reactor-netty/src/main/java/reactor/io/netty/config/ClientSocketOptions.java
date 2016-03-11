/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io.netty.config;

import java.util.function.Consumer;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;

/**
 * @author Jon Brisbin
 */
public class ClientSocketOptions extends CommonSocketOptions<ClientSocketOptions> {

	private Consumer<ChannelPipeline> pipelineConfigurer;
	private EventLoopGroup eventLoopGroup;

	public EventLoopGroup eventLoopGroup() {
		return eventLoopGroup;
	}

	public ClientSocketOptions eventLoopGroup(EventLoopGroup eventLoopGroup) {
		this.eventLoopGroup = eventLoopGroup;
		return this;
	}

	public Consumer<ChannelPipeline> pipelineConfigurer() {
		return pipelineConfigurer;
	}

	public ClientSocketOptions pipelineConfigurer(Consumer<ChannelPipeline> pipelineConfigurer) {
		this.pipelineConfigurer = pipelineConfigurer;
		return this;
	}
}
