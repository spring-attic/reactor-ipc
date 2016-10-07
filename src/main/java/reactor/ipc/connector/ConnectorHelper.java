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

package reactor.ipc.connector;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import reactor.core.Cancellation;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.ipc.Inbound;
import reactor.ipc.Ipc;
import reactor.ipc.Outbound;

abstract class ConnectorHelper {

	private ConnectorHelper() {
	}

	static <I, O, API> Mono<API> connect(Connector<I, O> connector,
			Supplier<?> receiverSupplier,
			Class<? extends API> api,
			BiConsumer<? super Inbound<I>, StreamEndpoint> decoder,
			Function<? super Outbound<O>, ? extends StreamRemote> encoder) {

		return Mono.create(new OnConnectorSubscribe<>(connector,
				receiverSupplier,
				api,
				decoder,
				encoder));
	}

	static final class OnConnectorSubscribe<I, O, API>
			implements Consumer<MonoSink<API>> {

		final Connector<I, O>                                       connector;
		final Supplier<?>                                           localSupplier;
		final Class<? extends API>                                  remoteApi;
		final String                                                endpointName;
		final BiConsumer<? super Inbound<I>, StreamEndpoint>        ipcReader;
		final Function<? super Outbound<O>, ? extends StreamRemote> ipcWriter;

		OnConnectorSubscribe(Connector<I, O> connector,
				Supplier<?> localSupplier,
				Class<? extends API> remoteApi,
				BiConsumer<? super Inbound<I>, StreamEndpoint> ipcReader,
				Function<? super Outbound<O>, ? extends StreamRemote> ipcWriter) {
			this.connector = Objects.requireNonNull(connector, "connector");
			this.endpointName = connector.getClass()
			                             .getSimpleName()
			                             .toLowerCase();
			this.ipcReader = ipcReader;
			this.ipcWriter = ipcWriter;
			this.localSupplier = localSupplier;
			this.remoteApi = remoteApi;
		}

		@Override
		public void accept(MonoSink<API> sink) {
			Object localAPI;

			if (localSupplier == null) {
				localAPI = null;
			}
			else {
				localAPI = Objects.requireNonNull(localSupplier.get(), "localSupplier");
			}

			Cancellation c = connector.newHandler((in, out) -> {
				Map<String, Object> clientMap;
				Map<String, Object> serverMap;

				StreamEndpointImpl[] am = {null};
				API api;
				final DirectProcessor<Void> closing;

				if (remoteApi != null) {
					clientMap = IpcServiceMapper.clientServiceMap(remoteApi);
					if (Cancellation.class.isAssignableFrom(remoteApi)) {
						closing = DirectProcessor.create();
					}
					else {
						closing = null;
					}
					api =
							remoteApi.cast(Proxy.newProxyInstance(remoteApi.getClassLoader(),
									new Class[]{remoteApi},
									(o, m, args) -> {
										String name = m.getName();
										Ipc a = m.getAnnotation(Ipc.class);
										if (a == null) {
											if (closing != null && m.getDeclaringClass()
											                        .equals(Cancellation.class)) {
												closing.onComplete();
												return null;
											}
											throw new IllegalArgumentException(
													"The method '" + m.getName() + "' is not annotated with Ipc");
										}
										String aname = a.name();
										if (!aname.isEmpty()) {
											name = aname;
										}

										Object action = clientMap.get(name);
										if (action == null) {
											throw new IllegalArgumentException(
													"The method '" + m.getName() + "' is not a proper Ipc method");
										}
										return IpcServiceMapper.dispatchClient(name,
												action,
												args,
												am[0]);
									}));
				}
				else {
					api = null;
					closing = null;
				}

				StreamContextImpl<API> ctx = new StreamContextImpl<>(api);
				StreamRemote streamRemote =
						Objects.requireNonNull(ipcWriter.apply(out), "remote");

				if (localAPI != null) {
					serverMap = IpcServiceMapper.serverServiceMap(localAPI);

					am[0] = new StreamEndpointImpl<>(endpointName,
							(streamId, function, iom) -> {
								Object action = serverMap.get(function);
								if (action == null) {
									throw new IllegalStateException("Function " + function + " not found");
								}
								return IpcServiceMapper.dispatchServer(streamId,
										action,
										iom,
										ctx);
							},
							streamRemote, in,
							() -> IpcServiceMapper.invokeDone(localAPI, ctx));

					in.inboundScheduler()
					  .schedule(() -> IpcServiceMapper.invokeInit(localAPI, ctx));
				}
				else {
					am[0] = new StreamEndpointImpl<>(endpointName,
							(streamId, function, iom) -> false,
							streamRemote,
							in,
							() -> {
							});
				}

				if (ipcReader != null) {
					ipcReader.accept(in, am[0]);
				}

				if (api != null) {
					sink.success(api);
				}

				return closing != null ? closing : Mono.never();
			})
			                          .subscribe(null, sink::error, sink::success);
			sink.setCancellation(c);
		}
	}
}
