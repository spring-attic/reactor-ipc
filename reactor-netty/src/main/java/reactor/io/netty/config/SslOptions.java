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

import java.io.File;
import java.util.Objects;
import java.util.function.Supplier;
import javax.net.ssl.TrustManager;

/**
 * Helper class encapsulating common SSL configuration options.
 *
 * @author Jon Brisbin
 */
public class SslOptions {

	private File   keystoreFile;
	private String keystorePasswd;
	private String keyManagerPasswd;
	private String keyManagerFactoryAlgorithm = "SunX509";
	private Supplier<TrustManager[]> trustManagers;
	private String                   trustManagerPasswd;
	private String trustManagerFactoryAlgorithm = "SunX509";
	private String sslProtocol                  = "TLS";

	public String keyManagerFactoryAlgorithm() {
		return keyManagerFactoryAlgorithm;
	}

	public SslOptions keyManagerFactoryAlgorithm(String keyManagerFactoryAlgorithm) {
		this.keyManagerFactoryAlgorithm =
				Objects.requireNonNull(keyManagerFactoryAlgorithm, "KeyManagerFactory algorithm cannot be null");
		return this;
	}

	public String keyManagerPasswd() {
		return keyManagerPasswd;
	}

	public SslOptions keyManagerPasswd(String keyManagerPasswd) {
		this.keyManagerPasswd = keyManagerPasswd;
		return this;
	}

	public String keystoreFile() {
		return (null != keystoreFile ? keystoreFile.getPath() : null);
	}

	public SslOptions keystoreFile(String keystoreFile) {
		this.keystoreFile = new File(keystoreFile);
		if(!this.keystoreFile.exists()) {
			throw new IllegalArgumentException("No keystore file found at path " + this.keystoreFile.getAbsolutePath());
		}
		return this;
	}

	public String keystorePasswd() {
		return keystorePasswd;
	}

	public SslOptions keystorePasswd(String keystorePasswd) {
		this.keystorePasswd = keystorePasswd;
		return this;
	}

	public String sslProtocol() {
		return sslProtocol;
	}

	public SslOptions sslProtocol(String sslProtocol) {
		this.sslProtocol = Objects.requireNonNull(sslProtocol, "SSL version cannot be null");
		return this;
	}

	public SslOptions toImmutable() {
		return new ImmutableSslOptions(this);
	}

	public String trustManagerFactoryAlgorithm() {
		return trustManagerFactoryAlgorithm;
	}

	public SslOptions trustManagerFactoryAlgorithm(String trustManagerFactoryAlgorithm) {
		this.trustManagerFactoryAlgorithm =
				Objects.requireNonNull(trustManagerFactoryAlgorithm, "TrustManagerFactory algorithm cannot be null");
		return this;
	}

	public String trustManagerPasswd() {
		return trustManagerPasswd;
	}

	public SslOptions trustManagerPasswd(String trustManagerPasswd) {
		this.trustManagerPasswd = trustManagerPasswd;
		return this;
	}

	public Supplier<TrustManager[]> trustManagers() {
		return trustManagers;
	}

	public SslOptions trustManagers(Supplier<TrustManager[]> trustManagers) {
		this.trustManagers = trustManagers;
		return this;
	}

	final static class ImmutableSslOptions extends SslOptions {

		final SslOptions sslOptions;

		public ImmutableSslOptions(SslOptions sslOptions) {
			this.sslOptions = sslOptions;
		}

		@Override
		public String keyManagerFactoryAlgorithm() {
			return sslOptions.keyManagerFactoryAlgorithm();
		}

		@Override
		public String keyManagerPasswd() {
			return sslOptions.keyManagerPasswd();
		}

		@Override
		public String keystoreFile() {
			return sslOptions.keystoreFile();
		}

		@Override
		public String keystorePasswd() {
			return sslOptions.keystorePasswd();
		}

		@Override
		public String sslProtocol() {
			return sslOptions.sslProtocol();
		}

		@Override
		public String trustManagerFactoryAlgorithm() {
			return sslOptions.trustManagerFactoryAlgorithm();
		}

		@Override
		public String trustManagerPasswd() {
			return sslOptions.trustManagerPasswd();
		}

		@Override
		public Supplier<TrustManager[]> trustManagers() {
			return sslOptions.trustManagers();
		}

		@Override
		public SslOptions keyManagerFactoryAlgorithm(String keyManagerFactoryAlgorithm) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions keyManagerPasswd(String keyManagerPasswd) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions keystoreFile(String keystoreFile) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions keystorePasswd(String keystorePasswd) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions sslProtocol(String sslProtocol) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions trustManagerFactoryAlgorithm(String trustManagerFactoryAlgorithm) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions trustManagerPasswd(String trustManagerPasswd) {
			throw new UnsupportedOperationException("Immutable Options");
		}

		@Override
		public SslOptions trustManagers(Supplier<TrustManager[]> trustManagers) {
			throw new UnsupportedOperationException("Immutable Options");
		}
	}

}
