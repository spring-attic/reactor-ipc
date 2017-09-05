/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.ipc.stream;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.context.Context;

@SuppressWarnings("unchecked")
abstract class IpcServiceMapper {

	final static Logger log = Loggers.getLogger(IpcServiceMapper.class);

	private IpcServiceMapper() {
	}

	public static void invokeInit(Object api, StreamContext<?> ctx) {
		for (Method m : api.getClass()
		                   .getMethods()) {
			if (m.isAnnotationPresent(IpcInit.class)) {
				if (m.getReturnType() == Void.TYPE) {
					if (m.getParameterCount() == 1 && StreamContext.class.isAssignableFrom(
							m.getParameterTypes()[0])) {

						try {
							m.invoke(api, ctx);
						}
						catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							if (log.isErrorEnabled()) {
								log.error("", e);
							}
							throw new IllegalStateException(e);
						}

						return;
					}
				}
				throw new IllegalStateException(
						"IpcInit method has to be void and accepting only a single StreamContext parameter");
			}
		}
	}

	public static void invokeDone(Object api, StreamContext<?> ctx) {
		for (Method m : api.getClass()
		                   .getMethods()) {
			if (m.isAnnotationPresent(IpcDone.class)) {
				if (m.getReturnType() == Void.TYPE) {
					if (m.getParameterCount() == 1 && StreamContext.class.isAssignableFrom(
							m.getParameterTypes()[0])) {

						try {
							m.invoke(api, ctx);
						}
						catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							if (log.isErrorEnabled()) {
								log.error("", e);
							}
							throw new IllegalStateException(e);
						}

						return;
					}
				}
				throw new IllegalStateException(
						"IpcInit method has to be void and accepting only a single StreamContext parameter");
			}
		}
	}

	public static Map<String, Object> serverServiceMap(Object api) {
		Map<String, Object> result = new HashMap<>();

		for (Method m : api.getClass()
		                   .getMethods()) {
			if (m.isAnnotationPresent(Ipc.class)) {
				Ipc a = m.getAnnotation(Ipc.class);

				String name = m.getName();

				String aname = a.name();
				if (!aname.isEmpty()) {
					name = aname;
				}

				Class<?> rt = m.getReturnType();

				if (rt == Void.TYPE) {
					int pc = m.getParameterCount();
					if (pc == 2) {
						if (StreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
							if (Publisher.class.isAssignableFrom(m.getParameterTypes()[1])) {
								if (Flux.class.equals(m.getParameterTypes()[1])) {
									result.put(name, new IpcServerReceiveFlux(m, api));
								}
								else if (Mono.class.equals(m.getParameterTypes()[1])) {
									result.put(name, new IpcServerReceiveMono(m, api));
								}
								else {
									result.put(name, new IpcServerReceive(m, api));
								}
							}
							else {
								throw new IllegalStateException(
										"Ipc annotated methods require a second Publisher as a parameter: " + m);
							}
						}
						else {
							throw new IllegalStateException(
									"Ipc annotated methods require a first StreamContext as a parameter: " + m);
						}
					}
					else {
						throw new IllegalStateException(
								"Ipc annotated methods require one StreamContext and one Publisher as a parameter: " + m);
					}
				}
				else if (Publisher.class.isAssignableFrom(rt)) {
					int pc = m.getParameterCount();
					if (pc == 1) {
						if (StreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
							result.put(name, new IpcServerSend(m, api));
						}
						else {
							throw new IllegalStateException(
									"Ipc annotated methods require at one StreamContext as a parameter: " + m);
						}
					}
					else if (pc == 2) {
						if (StreamContext.class.isAssignableFrom(m.getParameterTypes()[0])) {
							if (Publisher.class.isAssignableFrom(m.getParameterTypes()[1])) {
								if (Flux.class.equals(m.getParameterTypes()[1])) {
									result.put(name, new IpcServerMapFlux(m, api));
								}
								else if (Mono.class.equals(m.getParameterTypes()[1])) {
									result.put(name, new IpcServerMapMono(m, api));
								}
								else {
									result.put(name, new IpcServerMap(m, api));
								}
							}
							else {
								throw new IllegalStateException(
										"Ipc annotated methods require the second parameter to be Publisher.");
							}
						}
						else {
							throw new IllegalStateException(
									"Ipc annotated methods require the first parameter to be StreamContext.");
						}
					}
					else {
						throw new IllegalStateException(
								"Ipc annotated methods require one StreamContext and one Publisher as a parameter: " + m);
					}
				}
				else {
					throw new IllegalStateException(
							"Ipc annotated methods require Publisher: " + m);
				}
			}
		}

		return result;
	}

	public static Map<String, Object> clientServiceMap(Class<?> api) {
		Map<String, Object> result = new HashMap<>();

		for (Method m : api.getMethods()) {
			if (m.isAnnotationPresent(Ipc.class)) {
				Ipc a = m.getAnnotation(Ipc.class);

				String name = m.getName();

				String aname = a.name();
				if (!aname.isEmpty()) {
					name = aname;
				}

				if (result.containsKey(name)) {
					throw new IllegalStateException(
							"Overloads with the same target name are not supported");
				}

				Class<?> rt = m.getReturnType();

				if (rt == Void.TYPE) {
					int pc = m.getParameterCount();
					if (pc == 0) {
						throw new IllegalStateException(
								"Ipc annotated void methods require at least one parameter");
					}
					else if (pc == 1) {
						if (Function.class.isAssignableFrom(m.getParameterTypes()[0])) {
							String s = m.toGenericString();
							if(s.contains("<"+Flux.class.getName())){
								result.put(name, new IpcClientUmapFlux());
							}
							else if(s.contains("<"+Mono.class.getName())){
								result.put(name, new IpcClientUmapMono());
							}
							else {
								result.put(name, new IpcClientUmap());
							}
							continue;
						}
						else if (Publisher.class.isAssignableFrom(m.getParameterTypes()[0])) {
							result.put(name, new IpcClientSend());
							continue;
						}
					}
					throw new IllegalStateException(
							"Ipc annotated methods returning a void require 1 " + "parameter: " + m);
				}
				else if (Publisher.class.isAssignableFrom(rt)) {
					int pc = m.getParameterCount();
					if (pc > 1) {
						throw new IllegalStateException(
								"Ipc annotated methods returning a Publisher require 0 or 1 parameter: " + m);
					}
					if (pc == 0) {
						if (Flux.class.equals(rt)) {
							result.put(name, new IpcClientReceiveFlux());
						}
						else if (Mono.class.equals(rt)) {
							result.put(name, new IpcClientReceiveMono());
						}
						else {
							result.put(name, new IpcClientReceive());
						}
					}
					else {
						if (Publisher.class.isAssignableFrom(m.getParameterTypes()[0])) {

							if (Flux.class.equals(rt)) {
								result.put(name, new IpcClientMapFlux());
							}
							else if (Mono.class.equals(rt)) {
								result.put(name, new IpcClientMapMono());
							}
							else {
								result.put(name, new IpcClientMap());
							}
						}
						else {
							throw new IllegalStateException(
									"Ipc annotated methods returning a Publisher allows only Publisher as parameter: " + m);
						}
					}
				}
				else {
					throw new IllegalStateException(
							"Ipc annotated methods require Publisher: " + m);
				}
			}
		}

		return result;
	}

	public static boolean dispatchServer(long streamId,
			Object action,
			StreamOperationsImpl io,
			StreamContext<?> ctx) {
		if (action instanceof IpcServerSend) {
			IpcServerSend rpcServerSend = (IpcServerSend) action;
			return rpcServerSend.send(streamId, ctx, io);
		}
		else if (action instanceof IpcServerReceive) {
			IpcServerReceive rpcServerReceive = (IpcServerReceive) action;
			return rpcServerReceive.receive(streamId, ctx, io);
		}
		else if (action instanceof IpcServerMap) {
			IpcServerMap rpcServerMap = (IpcServerMap) action;
			return rpcServerMap.map(streamId, ctx, io);
		}
		if (log.isErrorEnabled()) {
			log.error("",
					new IllegalStateException("Unsupported action: " + action.getClass()));
		}
		return false;
	}

	public static Publisher<?> dispatchClient(String name,
			Object action,
			Object[] args,
			StreamOperationsImpl io) {
		if (action instanceof IpcClientSend) {
			if (args[0] == null) {
				throw new NullPointerException("The source Publisher is null");
			}
			IpcClientSend rpcSend = (IpcClientSend) action;
			rpcSend.send(name, (Publisher<?>) args[0], io);
			return null;
		}
		else if (action instanceof IpcClientReceive) {
			IpcClientReceive rpcReceive = (IpcClientReceive) action;
			return rpcReceive.receive(name, io);
		}
		else if (action instanceof IpcClientMap) {
			if (args[0] == null) {
				throw new NullPointerException("The source Publisher is null");
			}
			IpcClientMap rpcMap = (IpcClientMap) action;
			return rpcMap.map(name, (Publisher<?>) args[0], io);
		}
		else if (action instanceof IpcClientUmap) {
			if (args[0] == null) {
				throw new NullPointerException("The umapper function is null");
			}
			IpcClientUmap rpcUmap = (IpcClientUmap) action;
			Function<Publisher<?>, Publisher<?>> f =
					(Function<Publisher<?>, Publisher<?>>) args[0];
			rpcUmap.umap(name, f, io);
			return null;
		}
		throw new IllegalStateException("Unsupported action class: " + action.getClass());
	}

	static final class IpcClientSend {

		public static void sendStatic(String function,
				Publisher<?> values,
				StreamOperationsImpl io) {
			long streamId = io.newStreamId();

			SendSubscriber s = new SendSubscriber(io, streamId);
			io.registerSubscription(streamId, s);
			io.sendNew(streamId, function);

			values.subscribe(s);
		}

		public void send(String function, Publisher<?> values, StreamOperationsImpl io) {
			sendStatic(function, values, io);
		}

		static final class SendSubscriber extends Operators.DeferredSubscription
				implements CoreSubscriber<Object> {

			final StreamOperationsImpl io;

			final long streamId;

			boolean done;

			public SendSubscriber(StreamOperationsImpl io, long streamId) {
				this.io = io;
				this.streamId = streamId;
			}

			@Override
			public void onSubscribe(Subscription s) {
				super.set(s);
			}

			@Override
			public void onNext(Object t) {
				if (done) {
					return;
				}
				try {
					io.sendNext(streamId, t);
				}
				catch (IOException ex) {
					cancel();
					onError(ex);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (done) {
					Operators.onErrorDropped(t, Context.empty());
					return;
				}
				done = true;
				io.deregister(streamId);
				io.sendError(streamId, t);
			}

			@Override
			public void onComplete() {
				if (done) {
					return;
				}
				done = true;
				io.deregister(streamId);
				io.sendComplete(streamId);
			}
		}
	}

	static class IpcClientReceive {

		static final class IpcReceiveSubscription implements Subscription {

			final long streamId;

			final StreamOperationsImpl io;

			public IpcReceiveSubscription(long streamId, StreamOperationsImpl io) {
				this.streamId = streamId;
				this.io = io;
			}

			@Override
			public void request(long n) {
				if (Operators.validate(n)) {
					io.sendRequested(streamId, n);
				}
			}

			@Override
			public void cancel() {
				if (io.deregister(streamId)) {
					io.sendCancel(streamId, "");
				}
			}
		}

		public Publisher<?> receive(String function, StreamOperationsImpl io) {
			return s -> {
				long streamId = io.newStreamId();
				io.registerSubscriber(streamId, s);
				io.sendNew(streamId, function);

				s.onSubscribe(new IpcReceiveSubscription(streamId, io));
			};
		}

	}

	static final class IpcClientReceiveMono extends IpcClientReceive {

		@Override
		public Publisher<?> receive(String function, StreamOperationsImpl io) {
			return Mono.from(super.receive(function, io));
		}
	}

	static final class IpcClientReceiveFlux extends IpcClientReceive {

		@Override
		public Publisher<?> receive(String function, StreamOperationsImpl io) {
			return Flux.from(super.receive(function, io));
		}
	}

	static class IpcClientMap {

		public Publisher<?> map(String function,
				Publisher<?> values,
				StreamOperationsImpl io) {
			return s -> {
				long streamId = io.newStreamId();

				final AtomicInteger open = new AtomicInteger(2);

				IpcMapReceiverSubscriber receiver =
						new IpcMapReceiverSubscriber(s, streamId, open, io);

				IpcMapSubscriber sender = new IpcMapSubscriber(streamId, open, io);
				receiver.sender = sender;

				io.registerSubscriber(streamId, receiver);
				io.registerSubscription(streamId, sender);
				io.sendNew(streamId, function);

				s.onSubscribe(receiver.s);

				values.subscribe(sender);
			};
		}

		static final class IpcMapSubscriber extends Operators.DeferredSubscription
				implements CoreSubscriber<Object> {

			final long streamId;

			final AtomicInteger open;

			final StreamOperationsImpl io;

			boolean done;

			public IpcMapSubscriber(long streamId,
					AtomicInteger open,
					StreamOperationsImpl io) {
				this.streamId = streamId;
				this.open = open;
				this.io = io;
			}

			@Override
			public void onSubscribe(Subscription s) {
				super.set(s);
			}

			@Override
			public void onNext(Object t) {
				if (done) {
					return;
				}
				try {
					io.sendNext(streamId, t);
				}
				catch (IOException ex) {
					cancel();
					onError(ex);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (done) {
					Operators.onErrorDropped(t, Context.empty());
					return;
				}
				done = true;
				if (open.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				io.sendError(streamId, t);
			}

			@Override
			public void onComplete() {
				if (done) {
					return;
				}
				done = true;
				if (open.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				io.sendComplete(streamId);
			}
		}

		static final class IpcMapReceiverSubscriber
				implements CoreSubscriber<Object>, Subscription {

			final Subscriber<Object> actual;

			final long streamId;

			final AtomicInteger open;

			final StreamOperationsImpl io;

			Subscription s;

			IpcMapSubscriber sender;

			public IpcMapReceiverSubscriber(Subscriber<Object> actual,
					long streamId,
					AtomicInteger open,
					StreamOperationsImpl io) {
				this.actual = actual;
				this.streamId = streamId;
				this.open = open;
				this.io = io;
				this.s = new Subscription() {
					@Override
					public void request(long n) {
						innerRequest(n);
					}

					@Override
					public void cancel() {
						innerCancel();
					}
				};
			}

			void innerRequest(long n) {
				if (Operators.validate(n)) {
					io.sendRequested(streamId, n);
				}
			}

			void innerCancel() {
				if (open.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				io.sendCancel(streamId, "");
			}

			@Override
			public void onSubscribe(Subscription s) {
				// IO manager won't call this
			}

			@Override
			public void onNext(Object t) {
				actual.onNext(t);
			}

			@Override
			public void onError(Throwable t) {
				if (open.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				actual.onError(t);
			}

			@Override
			public void onComplete() {
				if (open.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				actual.onComplete();
			}

			@Override
			public void request(long n) {
				sender.request(n);
			}

			@Override
			public void cancel() {
				sender.cancel();
			}
		}
	}

	static final class IpcClientMapMono extends IpcClientMap {

		@Override
		public Publisher<?> map(String function,
				Publisher<?> values,
				StreamOperationsImpl io) {
			return Mono.from(super.map(function, values, io));
		}
	}

	static final class IpcClientMapFlux extends IpcClientMap {

		@Override
		public Publisher<?> map(String function,
				Publisher<?> values,
				StreamOperationsImpl io) {
			return Flux.from(super.map(function, values, io));
		}
	}

	static class IpcClientUmap {

		Publisher<?> producer(IpcUmapReceiver receiver){
			AtomicBoolean once = new AtomicBoolean();
			return s -> {
				if (once.compareAndSet(false, true)) {
					receiver.actual = s;
					s.onSubscribe(receiver.s);
				}
				else {
					Operators.error(s,
							new IllegalStateException("Only one subscriber allowed"));
				}
			};
		}

		final void umap(String function,
				Function<Publisher<?>, Publisher<?>> mapper,
				StreamOperationsImpl io) {

			long streamId = io.newStreamId();

			AtomicBoolean onceInner = new AtomicBoolean();

			IpcUmapReceiver receiver = new IpcUmapReceiver(streamId, io, onceInner);

			receiver.provider = new IpcUmapProvider(streamId, io, onceInner);

			io.registerSubscriber(streamId, receiver);
			io.registerSubscription(streamId, receiver);

			io.sendNew(streamId, function);

			Publisher<?> p = producer(receiver);

			Publisher<?> u;

			try {
				u = mapper.apply(p);
			}
			catch (Throwable ex) {
				u = w -> {
					Operators.error(w, ex);
				};
			}

			if (u == null) {
				u = w -> {
					Operators.error(w,
							new NullPointerException(
									"The umapper returned a null Publisher"));
				};
			}

			u.subscribe(receiver.provider);
		}

		static final class IpcUmapReceiver implements CoreSubscriber<Object>, Subscription {

			final long streamId;

			final StreamOperationsImpl io;

			final AtomicBoolean once;

			Subscriber<Object> actual;

			IpcUmapProvider provider;

			Subscription s;

			public IpcUmapReceiver(long streamId,
					StreamOperationsImpl io,
					AtomicBoolean once) {
				this.streamId = streamId;
				this.io = io;
				this.once = once;
				this.s = new Subscription() {
					@Override
					public void request(long n) {
						if (Operators.validate(n)) {
							io.sendRequested(streamId, n);
						}
					}

					@Override
					public void cancel() {
						if (once.compareAndSet(false, true)) {
							io.sendCancel(streamId, "");
						}
					}
				};
			}

			@Override
			public void onSubscribe(Subscription s) {
				// not called
			}

			@Override
			public void onNext(Object t) {
				actual.onNext(t);
			}

			@Override
			public void onError(Throwable t) {
				once.set(true);
				actual.onError(t);
			}

			@Override
			public void onComplete() {
				once.set(true);
				actual.onComplete();
			}

			@Override
			public void request(long n) {
				provider.request(n);
			}

			@Override
			public void cancel() {
				provider.cancel();
			}
		}

		static final class IpcUmapProvider extends Operators.DeferredSubscription
				implements CoreSubscriber<Object> {

			final long streamId;

			final StreamOperationsImpl io;

			final AtomicBoolean once;

			boolean done;

			public IpcUmapProvider(long streamId,
					StreamOperationsImpl io,
					AtomicBoolean once) {
				this.streamId = streamId;
				this.io = io;
				this.once = once;
			}

			@Override
			public void onSubscribe(Subscription s) {
				set(s);
			}

			@Override
			public void onNext(Object t) {
				if (done) {
					return;
				}
				try {
					io.sendNext(streamId, t);
				}
				catch (IOException ex) {
					onError(ex);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (done) {
					Operators.onErrorDropped(t, Context.empty());
					return;
				}
				done = true;
				cancel();
				io.deregister(streamId);
				if (once.compareAndSet(false, true)) {
					io.sendCancel(streamId, "");
				}
				io.sendError(streamId, t);
			}

			@Override
			public void onComplete() {
				if (done) {
					return;
				}
				done = true;
				cancel();
				io.deregister(streamId);
				if (once.compareAndSet(false, true)) {
					io.sendCancel(streamId, "");
				}
				io.sendComplete(streamId);
			}
		}
	}

	static final class IpcClientUmapFlux extends IpcClientUmap {
		@Override
		Publisher<?> producer(IpcUmapReceiver receiver) {
			return Flux.from(super.producer(receiver));
		}
	}

	static final class IpcClientUmapMono extends IpcClientUmap {
		@Override
		Publisher<?> producer(IpcUmapReceiver receiver) {
			return Mono.from(super.producer(receiver));
		}
	}

	static final class IpcServerSend {

		final Method m;

		final Object instance;

		public IpcServerSend(Method m, Object instance) {
			this.m = m;
			this.instance = instance;
		}

		public boolean send(long streamId, StreamContext<?> ctx, StreamOperationsImpl io) {
			Publisher<?> output;
			try {
				output = (Publisher<?>) m.invoke(instance, ctx);
			}
			catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("", ex);
				}
				io.sendError(streamId, ex);
				return true;
			}

			if (output == null) {
				io.sendError(streamId,
						new IllegalStateException(
								"The service implementation returned a null Publisher"));
				return true;
			}

			ServerSendSubscriber parent = new ServerSendSubscriber(streamId, io);
			io.registerSubscription(streamId, parent);

			output.subscribe(parent);

			return true;
		}

		static final class ServerSendSubscriber extends Operators.DeferredSubscription
				implements CoreSubscriber<Object> {

			final long streamId;

			final StreamOperationsImpl io;

			boolean done;

			public ServerSendSubscriber(long streamId, StreamOperationsImpl io) {
				this.streamId = streamId;
				this.io = io;
			}

			@Override
			public void onSubscribe(Subscription s) {
				set(s);
			}

			@Override
			public void onNext(Object t) {
				if (done) {
					return;
				}
				try {
					io.sendNext(streamId, t);
				}
				catch (IOException ex) {
					cancel();
					onError(ex);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (done) {
					Operators.onErrorDropped(t, Context.empty());
					return;
				}
				done = true;
				io.deregister(streamId);
				io.sendError(streamId, t);
			}

			@Override
			public void onComplete() {
				if (done) {
					return;
				}
				done = true;
				io.deregister(streamId);
				io.sendComplete(streamId);
			}
		}
	}

	static class IpcServerReceive {

		final Method m;

		final Object instance;

		public IpcServerReceive(Method m, Object instance) {
			this.m = m;
			this.instance = instance;
		}

		Publisher<?> producer(long streamId, StreamOperationsImpl io) {
			ServerReceiveSubscriber parent = new ServerReceiveSubscriber(streamId, io);

			AtomicBoolean once = new AtomicBoolean();
			return s -> {
				if (once.compareAndSet(false, true)) {
					parent.actual = s;
					io.registerSubscriber(streamId, parent);
					s.onSubscribe(parent);
				}
				else {
					Operators.error(s,
							new IllegalStateException(
									"This Publisher allows only a single subscriber"));
				}
			};
		}

		final boolean receive(long streamId,
				StreamContext<?> ctx,
				StreamOperationsImpl io) {

			Publisher<?> p = producer(streamId, io);

			try {
				m.invoke(instance, ctx, p);
			}
			catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("", ex);
				}

				io.sendCancel(streamId, ex.toString());
			}
			return true;
		}

		static final class ServerReceiveSubscriber
				implements CoreSubscriber<Object>, Subscription {

			final long streamId;

			final StreamOperationsImpl io;

			Subscriber<Object> actual;

			public ServerReceiveSubscriber(long streamId, StreamOperationsImpl io) {
				this.streamId = streamId;
				this.io = io;
			}

			@Override
			public void onSubscribe(Subscription s) {
				// not called by the IO manager
			}

			@Override
			public void onNext(Object t) {
				actual.onNext(t);
			}

			@Override
			public void onError(Throwable t) {
				io.deregister(streamId);
				actual.onError(t);
			}

			@Override
			public void onComplete() {
				io.deregister(streamId);
				actual.onComplete();
			}

			@Override
			public void request(long n) {
				if (Operators.validate(n)) {
					io.sendRequested(streamId, n);
				}
			}

			@Override
			public void cancel() {
				if (io.deregister(streamId)) {
					io.sendCancel(streamId, "");
				}
			}
		}
	}

	static final class IpcServerReceiveFlux extends IpcServerReceive {

		public IpcServerReceiveFlux(Method m, Object instance) {
			super(m, instance);
		}

		@Override
		Publisher<?> producer(long streamId, StreamOperationsImpl io) {
			return Flux.from(super.producer(streamId, io));
		}
	}

	static final class IpcServerReceiveMono extends IpcServerReceive {

		public IpcServerReceiveMono(Method m, Object instance) {
			super(m, instance);
		}

		@Override
		Publisher<?> producer(long streamId, StreamOperationsImpl io) {
			return Mono.from(super.producer(streamId, io));
		}
	}

	static class IpcServerMap {

		final Method m;

		final Object instance;

		public IpcServerMap(Method m, Object instance) {
			this.m = m;
			this.instance = instance;
		}

		Publisher<?> producer(long streamId,
				AtomicInteger innerOnce,
				ServerSendSubscriber sender,
				StreamOperationsImpl io) {
			ServerMapSubscriber parent = new ServerMapSubscriber(streamId, io, innerOnce);
			parent.sender = sender;

			io.registerSubscriber(streamId, parent);
			io.registerSubscription(streamId, sender);

			AtomicBoolean once = new AtomicBoolean();

			return s -> {
				if (once.compareAndSet(false, true)) {
					parent.actual = s;
					s.onSubscribe(parent.s);
				}
				else {
					Operators.error(s,
							new IllegalStateException(
									"This Publisher allows only a single subscriber"));
				}
			};
		}

		final boolean map(long streamId, StreamContext<?> ctx, StreamOperationsImpl io) {
			AtomicInteger innerOnce = new AtomicInteger(2);
			ServerSendSubscriber sender =
					new ServerSendSubscriber(streamId, io, innerOnce);

			Publisher<?> p = producer(streamId, innerOnce, sender, io);

			Publisher<?> u;
			try {
				u = (Publisher<?>) m.invoke(instance, ctx, p);
			}
			catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("", ex);
				}
				u = s -> Operators.error(s, ex);
			}

			if (u == null) {
				u = s -> Operators.error(s,
						new NullPointerException(
								"The service implementation returned a null Publisher"));
			}

			u.subscribe(sender);

			return true;
		}

		static final class ServerMapSubscriber
				implements CoreSubscriber<Object>, Subscription {

			final long streamId;

			final StreamOperationsImpl io;

			final AtomicInteger once;

			final Subscription s;

			Subscriber<Object> actual;

			ServerSendSubscriber sender;

			public ServerMapSubscriber(long streamId,
					StreamOperationsImpl io,
					AtomicInteger once) {
				this.streamId = streamId;
				this.io = io;
				this.once = once;
				this.s = new Subscription() {

					@Override
					public void request(long n) {
						innerRequest(n);
					}

					@Override
					public void cancel() {
						innerCancel();
					}

				};
			}

			@Override
			public void onSubscribe(Subscription s) {
				// IO won't call this
			}

			@Override
			public void onNext(Object t) {
				actual.onNext(t);
			}

			@Override
			public void onError(Throwable t) {
				if (once.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				actual.onError(t);
			}

			@Override
			public void onComplete() {
				if (once.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				actual.onComplete();
			}

			public void innerRequest(long n) {
				if (Operators.validate(n)) {
					io.sendRequested(streamId, n);
				}
			}

			public void innerCancel() {
				if (once.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				io.sendCancel(streamId, "");
			}

			@Override
			public void request(long n) {
				sender.request(n);
			}

			@Override
			public void cancel() {
				sender.cancel();
			}

		}

		static final class ServerSendSubscriber extends Operators.DeferredSubscription
				implements CoreSubscriber<Object> {

			final long streamId;

			final StreamOperationsImpl io;

			final AtomicInteger once;

			boolean done;

			public ServerSendSubscriber(long streamId,
					StreamOperationsImpl io,
					AtomicInteger once) {
				this.streamId = streamId;
				this.io = io;
				this.once = once;
			}

			@Override
			public void onSubscribe(Subscription s) {
				set(s);
			}

			@Override
			public void onNext(Object t) {
				if (done) {
					return;
				}
				try {
					io.sendNext(streamId, t);
				}
				catch (IOException ex) {
					cancel();
					onError(ex);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (done) {
					Operators.onErrorDropped(t, Context.empty());
					return;
				}
				done = true;
				if (once.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				io.sendError(streamId, t);
			}

			@Override
			public void onComplete() {
				if (done) {
					return;
				}
				done = true;
				if (once.decrementAndGet() == 0) {
					io.deregister(streamId);
				}
				io.sendComplete(streamId);
			}
		}
	}

	static final class IpcServerMapMono extends IpcServerMap {

		public IpcServerMapMono(Method m, Object instance) {
			super(m, instance);
		}

		@Override
		Publisher<?> producer(long streamId,
				AtomicInteger innerOnce,
				ServerSendSubscriber sender,
				StreamOperationsImpl io) {
			return Mono.from(super.producer(streamId, innerOnce, sender, io));
		}
	}

	static final class IpcServerMapFlux extends IpcServerMap {

		public IpcServerMapFlux(Method m, Object instance) {
			super(m, instance);
		}

		@Override
		Publisher<?> producer(long streamId,
				AtomicInteger innerOnce,
				ServerSendSubscriber sender,
				StreamOperationsImpl io) {
			return Flux.from(super.producer(streamId, innerOnce, sender, io));
		}
	}
}
