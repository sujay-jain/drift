/*
 * Copyright (C) 2013 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.drift.transport.netty.client;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import java.io.File;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class DriftNettyConnectionFactoryConfig
{
    private static final int DEFAULT_THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;

    private int threadCount = DEFAULT_THREAD_COUNT;

    private boolean connectionPoolEnabled = true;
    private int connectionPoolMaxSize = 1000;
    private Duration connectionPoolIdleTimeout = new Duration(1, MINUTES);

    private Duration sslContextRefreshTime = new Duration(1, MINUTES);
    private HostAndPort socksProxy;
    private boolean nativeTransportEnabled;
    private List<String> ciphers = ImmutableList.of();
    private File trustCertificate;
    private File key;
    private String keyPassword;
    private long sessionCacheSize = 10_000;
    private Duration sessionTimeout = new Duration(1, DAYS);

    public int getThreadCount()
    {
        return threadCount;
    }

    @Config("thrift.client.thread-count")
    public DriftNettyConnectionFactoryConfig setThreadCount(int threadCount)
    {
        this.threadCount = threadCount;
        return this;
    }

    public boolean isConnectionPoolEnabled()
    {
        return connectionPoolEnabled;
    }

    @Config("thrift.client.connection-pool.enabled")
    public DriftNettyConnectionFactoryConfig setConnectionPoolEnabled(boolean connectionPoolEnabled)
    {
        this.connectionPoolEnabled = connectionPoolEnabled;
        return this;
    }

    @Min(1)
    public int getConnectionPoolMaxSize()
    {
        return connectionPoolMaxSize;
    }

    @Config("thrift.client.connection-pool.max-size")
    public DriftNettyConnectionFactoryConfig setConnectionPoolMaxSize(int connectionPoolMaxSize)
    {
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        return this;
    }

    @MinDuration("1s")
    public Duration getConnectionPoolIdleTimeout()
    {
        return connectionPoolIdleTimeout;
    }

    @Config("thrift.client.connection-pool.idle-timeout")
    public DriftNettyConnectionFactoryConfig setConnectionPoolIdleTimeout(Duration connectionPoolIdleTimeout)
    {
        this.connectionPoolIdleTimeout = connectionPoolIdleTimeout;
        return this;
    }

    @MinDuration("1s")
    public Duration getSslContextRefreshTime()
    {
        return sslContextRefreshTime;
    }

    @Config("thrift.client.ssl-context.refresh-time")
    public DriftNettyConnectionFactoryConfig setSslContextRefreshTime(Duration sslContextRefreshTime)
    {
        this.sslContextRefreshTime = sslContextRefreshTime;
        return this;
    }

    public HostAndPort getSocksProxy()
    {
        return socksProxy;
    }

    @Config("thrift.client.socks-proxy")
    public DriftNettyConnectionFactoryConfig setSocksProxy(HostAndPort socksProxy)
    {
        this.socksProxy = socksProxy;
        return this;
    }

    public boolean isNativeTransportEnabled()
    {
        return nativeTransportEnabled;
    }

    @Config("thrift.client.native-transport.enabled")
    @ConfigDescription("Enable Native Transport")
    public DriftNettyConnectionFactoryConfig setNativeTransportEnabled(boolean nativeTransportEnabled)
    {
        this.nativeTransportEnabled = nativeTransportEnabled;
        return this;
    }

    public File getTrustCertificate()
    {
        return trustCertificate;
    }

    @Config("thrift.client.ssl.trust-certificate")
    public DriftNettyConnectionFactoryConfig setTrustCertificate(File trustCertificate)
    {
        this.trustCertificate = trustCertificate;
        return this;
    }

    public File getKey()
    {
        return key;
    }

    @Config("thrift.client.ssl.key")
    public DriftNettyConnectionFactoryConfig setKey(File key)
    {
        this.key = key;
        return this;
    }

    public String getKeyPassword()
    {
        return keyPassword;
    }

    @Config("thrift.client.ssl.key-password")
    public DriftNettyConnectionFactoryConfig setKeyPassword(String keyPassword)
    {
        this.keyPassword = keyPassword;
        return this;
    }

    public long getSessionCacheSize()
    {
        return sessionCacheSize;
    }

    @Config("thrift.client.ssl.session-cache-size")
    public DriftNettyConnectionFactoryConfig setSessionCacheSize(long sessionCacheSize)
    {
        this.sessionCacheSize = sessionCacheSize;
        return this;
    }

    public Duration getSessionTimeout()
    {
        return sessionTimeout;
    }

    @Config("thrift.client.ssl.session-timeout")
    public DriftNettyConnectionFactoryConfig setSessionTimeout(Duration sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
        return this;
    }

    public List<String> getCiphers()
    {
        return ciphers;
    }

    @Config("thrift.client.ssl.ciphers")
    public DriftNettyConnectionFactoryConfig setCiphers(String ciphers)
    {
        this.ciphers = Splitter
                .on(',')
                .trimResults()
                .omitEmptyStrings()
                .splitToList(requireNonNull(ciphers, "ciphers is null"));
        return this;
    }
}
