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

package reactor.ipc.stream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class StreamContextImpl<T> implements StreamContext<T> {

	final ConcurrentMap<CharSequence, Object> map;

	final T remoteAPI;

	public StreamContextImpl(T remoteAPI) {
		this.remoteAPI = remoteAPI;
		this.map = new ConcurrentHashMap<>();
	}

	@Override
	public void set(CharSequence attribute, Object o) {
		map.put(attribute, o);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> U get(CharSequence attribute) {
		return (U) map.get(attribute);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <U> U get(CharSequence attribute, U defaultValue) {
		return (U) map.getOrDefault(attribute, defaultValue);
	}

	@Override
	public void remove(CharSequence attribute) {
		map.remove(attribute);
	}

	@Override
	public boolean has(CharSequence attribute) {
		return map.containsKey(attribute);
	}

	@Override
	public T remoteAPI() {
		return remoteAPI;
	}

}
