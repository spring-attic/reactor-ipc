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

package reactor.io.netty.impl.netty.http;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import reactor.io.netty.http.model.ResponseHeaders;

/**
 * Store cookies for the http channel. TODO Need to refactor when switching to 4.1 to use more utils
 */
public class Cookies {

	final static int NOT_READ = 0;
	final static int READING  = 1;
	final static int READ     = 2;

	final HttpHeaders nettyHeaders;
	final String      cookiesHeaderName;
	final boolean     isClientChannel;

	Map<String, Set<reactor.io.netty.http.model.Cookie>> cachedCookies;

	volatile     int                                state = 0;
	static final AtomicIntegerFieldUpdater<Cookies> STATE =
			AtomicIntegerFieldUpdater.newUpdater(Cookies.class, "state");

	private Cookies(HttpHeaders nettyHeaders, String cookiesHeaderName, boolean isClientChannel) {
		this.nettyHeaders = nettyHeaders;
		this.cookiesHeaderName = cookiesHeaderName;
		this.isClientChannel = isClientChannel;
		cachedCookies = Collections.emptyMap();
	}

	public Map<String, Set<reactor.io.netty.http.model.Cookie>> getCachedCookies() {
		if (!STATE.compareAndSet(this, NOT_READ, READING)) {
			for (; ; ) {
				if (state == READ) {
					return cachedCookies;
				}
			}
		}

		List<String> allCookieHeaders = nettyHeaders.getAll(cookiesHeaderName);
		Map<String, Set<reactor.io.netty.http.model.Cookie>> cookies = new HashMap<>();
		for (String aCookieHeader : allCookieHeaders) {
			Set<Cookie> decode;
			if (isClientChannel) {
				final Cookie c = ClientCookieDecoder.STRICT.decode(aCookieHeader);
				Set<reactor.io.netty.http.model.Cookie> existingCookiesOfName = cookies.get(c.name());
				if (null == existingCookiesOfName) {
					existingCookiesOfName = new HashSet<>();
					cookies.put(c.name(), existingCookiesOfName);
				}
				existingCookiesOfName.add(new NettyReadCookie(c));
			}
			else {
				decode = ServerCookieDecoder.STRICT.decode(aCookieHeader);
				for (Cookie cookie : decode) {
					Set<reactor.io.netty.http.model.Cookie> existingCookiesOfName = cookies.get(cookie.name());
					if (null == existingCookiesOfName) {
						existingCookiesOfName = new HashSet<>();
						cookies.put(cookie.name(), existingCookiesOfName);
					}
					existingCookiesOfName.add(new NettyReadCookie(cookie));
				}
			}
		}
		cachedCookies = Collections.unmodifiableMap(cookies);
		state = READ;
		return cachedCookies;
	}

	public static Cookies newClientResponseHolder(HttpHeaders headers) {
		return new Cookies(headers, ResponseHeaders.SET_COOKIE, true);
	}

	public static Cookies newServerRequestHolder(HttpHeaders headers) {
		return new Cookies(headers, reactor.io.netty.http.model.HttpHeaders.COOKIE, false);
	}

	static final class NettyWriteCookie implements Cookie {

		final reactor.io.netty.http.model.Cookie c;

		public NettyWriteCookie(reactor.io.netty.http.model.Cookie c) {
			this.c = c;
		}

		@Override
		public String name() {
			return c.name();
		}

		@Override
		public String value() {
			return c.value();
		}

		@Override
		public void setValue(String value) {
			c.value(value);
		}

		@Override
		public boolean wrap() {
			return c.wrap();
		}

		@Override
		public void setWrap(boolean wrap) {
			c.wrap(wrap);
		}

		@Override
		public String domain() {
			return c.domain();
		}

		@Override
		public void setDomain(String domain) {
			c.domain(domain);
		}

		@Override
		public String path() {
			return c.path();
		}

		@Override
		public void setPath(String path) {
			c.path(path);
		}

		@Override
		public long maxAge() {
			return c.maxAge() <= -1 ? Long.MIN_VALUE : c.maxAge();
		}

		@Override
		public void setMaxAge(long maxAge) {
			c.maxAge(maxAge);
		}

		@Override
		public boolean isSecure() {
			return c.secure();
		}

		@Override
		public void setSecure(boolean secure) {
			c.secure(secure);
		}

		@Override
		public boolean isHttpOnly() {
			return c.httpOnly();
		}

		@Override
		public void setHttpOnly(boolean httpOnly) {
			c.httpOnly(httpOnly);
		}

		@Override
		public int compareTo(Cookie o) {
			return name().compareTo(o.name());
		}
	}

	static final class NettyReadCookie extends reactor.io.netty.http.model.Cookie {

		final Cookie c;

		public NettyReadCookie(Cookie c) {
			this.c = c;
		}

		@Override
		public String name() {
			return c.name();
		}

		@Override
		public String path() {
			return c.path();
		}

		@Override
		public String value() {
			return c.value();
		}

		@Override
		public String domain() {
			return c.domain();
		}

		@Override
		public boolean secure() {
			return c.isSecure();
		}

		@Override
		public boolean httpOnly() {
			return c.isHttpOnly();
		}

		@Override
		public boolean wrap() {
			return c.wrap();
		}

		@Override
		public long maxAge() {
			return c.maxAge();
		}

		@Override
		public void wrap(boolean wrap) {

		}

		@Override
		public void secure(boolean secure) {

		}

		@Override
		public void httpOnly(boolean httpOnly) {

		}

		@Override
		public void maxAge(long age) {

		}

		@Override
		public void path(String path) {

		}

		@Override
		public void value(String value) {

		}

		@Override
		public void domain(String domain) {

		}
	}
}
