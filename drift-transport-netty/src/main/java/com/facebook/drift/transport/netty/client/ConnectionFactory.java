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

import com.facebook.airlift.log.Logger;
import com.facebook.drift.protocol.TTransportException;
import com.facebook.drift.transport.client.Address;
import com.facebook.drift.transport.netty.ssl.SslContextFactory;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Ints.saturatedCast;
import static io.netty.channel.ChannelOption.ALLOCATOR;
import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static java.util.Objects.requireNonNull;

class ConnectionFactory
        implements ConnectionManager
{
    private static final Logger log = Logger.get(ConnectionFactory.class);

    private final EventLoopGroup group;
    private final SslContextFactory sslContextFactory;
    private final ByteBufAllocator allocator;
    private DriftNettyConnectionFactoryConfig connectionFactoryConfig;

    ConnectionFactory(
            EventLoopGroup group,
            SslContextFactory sslContextFactory,
            ByteBufAllocator allocator,
            DriftNettyConnectionFactoryConfig connectionFactoryConfig)
    {
        this.group = requireNonNull(group, "group is null");
        this.sslContextFactory = requireNonNull(sslContextFactory, "sslContextFactory is null");
        this.allocator = requireNonNull(allocator, "allocator is null");
        this.connectionFactoryConfig = requireNonNull(connectionFactoryConfig, "connectionFactoryConfig is null");
    }

    @Override
    public Future<Channel> getConnection(ConnectionParameters connectionParameters, Address address)
    {
        Class socketChannelClass = NioSocketChannel.class;
        if (connectionFactoryConfig.isNativeTransportEnabled()) {
            checkState(Epoll.isAvailable(), "native transport is not available");
            socketChannelClass = EpollSocketChannel.class;
        }

        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(socketChannelClass)
                    .option(ALLOCATOR, allocator)
                    .option(CONNECT_TIMEOUT_MILLIS, saturatedCast(connectionParameters.getConnectTimeout().toMillis()))
                    .handler(new ThriftClientInitializer(
                            connectionParameters.getTransport(),
                            connectionParameters.getProtocol(),
                            connectionParameters.getMaxFrameSize(),
                            connectionParameters.getRequestTimeout(),
                            connectionParameters.getSocksProxy(),
                            getSslContextSupplier(address.isEncryptionRequired(), connectionParameters)));

            Promise<Channel> promise = group.next().newPromise();
            promise.setUncancellable();
            bootstrap.connect(new InetSocketAddress(address.getHostAndPort().getHost(), address.getHostAndPort().getPort()))
                    .addListener((ChannelFutureListener) channelFuture -> notifyConnect(channelFuture, promise));
            return promise;
        }
        catch (Throwable e) {
            return group.next().newFailedFuture(new TTransportException(e));
        }
    }

    private static void notifyConnect(ChannelFuture channelFuture, Promise<Channel> promise)
    {
        if (channelFuture.isSuccess()) {
            Channel channel = channelFuture.channel();
            if (!promise.trySuccess(channel)) {
                // Promise was completed in the meantime (likely cancelled), just release the channel again
                channel.close();
            }
        }
        else {
            promise.tryFailure(channelFuture.cause());
        }
    }

    @Override
    public void returnConnection(Channel connection)
    {
        connection.close();
    }

    @Override
    public void close() {}

    private Optional<Supplier<SslContext>> getSslContextSupplier(boolean encryptionRequired, ConnectionParameters connectionParameters)
    {
        if (encryptionRequired) {
            log.info("banana encryption required. SSL Context Params %s", connectionParameters.getSslContextParameters().toString());
            return connectionParameters.getSslContextParameters().map(sslContextFactory::get);
        }
        else {
            return Optional.empty();
        }
    }
}
