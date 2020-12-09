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
package com.facebook.drift.client;

import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.client.stats.MethodInvocationStat;
import com.facebook.drift.client.stats.MethodInvocationStatsFactory;
import com.facebook.drift.client.stats.NullMethodInvocationStat;
import com.facebook.drift.client.stats.NullMethodInvocationStatsFactory;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.metadata.ThriftMethodMetadata;
import com.facebook.drift.codec.metadata.ThriftServiceMetadata;
import com.facebook.drift.transport.MethodMetadata;
import com.facebook.drift.transport.client.Address;
import com.facebook.drift.transport.client.DriftClientConfig;
import com.facebook.drift.transport.client.MethodInvoker;
import com.facebook.drift.transport.client.MethodInvokerFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.facebook.drift.client.ExceptionClassifier.NORMAL_RESULT;
import static com.facebook.drift.client.FilteredMethodInvoker.createFilteredMethodInvoker;
import static com.facebook.drift.transport.MethodMetadata.toMethodMetadata;
import static com.google.common.reflect.Reflection.newProxy;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class DriftClientFactory
{
    private static final int RETRY_THREADS = 16;
    private final ThriftCodecManager codecManager;
    private final Supplier<MethodInvoker> methodInvokerSupplier;
    private final AddressSelector<? extends Address> addressSelector;
    private final ExceptionClassifier exceptionClassifier;
    private final ConcurrentMap<Class<?>, ThriftServiceMetadata> serviceMetadataCache = new ConcurrentHashMap<>();
    private final MethodInvocationStatsFactory methodInvocationStatsFactory;

    public DriftClientFactory(
            ThriftCodecManager codecManager,
            Supplier<MethodInvoker> methodInvokerSupplier,
            AddressSelector<? extends Address> addressSelector,
            ExceptionClassifier exceptionClassifier,
            MethodInvocationStatsFactory methodInvocationStatsFactory)
    {
        this.codecManager = requireNonNull(codecManager, "codecManager is null");
        this.methodInvokerSupplier = requireNonNull(methodInvokerSupplier, "methodInvokerSupplier is null");
        this.addressSelector = requireNonNull(addressSelector, "addressSelector is null");
        this.exceptionClassifier = exceptionClassifier;
        this.methodInvocationStatsFactory = requireNonNull(methodInvocationStatsFactory, "methodInvocationStatsFactory is null");
    }

    public DriftClientFactory(
            ThriftCodecManager codecManager,
            MethodInvokerFactory<?> invokerFactory,
            AddressSelector<? extends Address> addressSelector,
            ExceptionClassifier exceptionClassifier)
    {
        this(
                codecManager,
                () -> invokerFactory.createMethodInvoker(null),
                addressSelector,
                exceptionClassifier,
                new NullMethodInvocationStatsFactory());
    }

    public DriftClientFactory(
            ThriftCodecManager codecManager,
            MethodInvokerFactory<?> invokerFactory,
            AddressSelector<? extends Address> addressSelector)
    {
        this(codecManager, invokerFactory, addressSelector, NORMAL_RESULT);
    }

    public <T> DriftClient<T> createDriftClient(Class<T> clientInterface)
    {
        return createDriftClient(clientInterface, Optional.empty(), ImmutableList.of(), new DriftClientConfig());
    }

    public <T> DriftClient<T> createDriftClient(
            Class<T> clientInterface,
            Optional<Class<? extends Annotation>> qualifierAnnotation,
            List<MethodInvocationFilter> filters,
            DriftClientConfig config)
    {
        ThriftServiceMetadata serviceMetadata = serviceMetadataCache.computeIfAbsent(
                clientInterface,
                clazz -> new ThriftServiceMetadata(clazz, codecManager.getCatalog()));

        MethodInvoker invoker = createFilteredMethodInvoker(filters, methodInvokerSupplier.get());

        Optional<String> qualifier = qualifierAnnotation.map(Class::getSimpleName);

        ImmutableMap.Builder<Method, DriftMethodHandler> builder = ImmutableMap.builder();
        for (ThriftMethodMetadata method : serviceMetadata.getMethods()) {
            MethodMetadata metadata = toMethodMetadata(codecManager, method);

            RetryPolicy retryPolicy = new RetryPolicy(config, exceptionClassifier);

            MethodInvocationStat statHandler;
            if (config.isStatsEnabled()) {
                statHandler = methodInvocationStatsFactory.getStat(serviceMetadata, qualifier, metadata);
            }
            else {
                statHandler = new NullMethodInvocationStat();
            }

            ExecutorService retryService = newFixedThreadPool(config.getRetryThreads());
            DriftMethodHandler handler = new DriftMethodHandler(metadata, method.getHeaderParameters(), invoker, method.isAsync(), addressSelector, retryPolicy, statHandler, retryService);
            builder.put(method.getMethod(), handler);
        }
        Map<Method, DriftMethodHandler> methods = builder.build();

        return (context, headers) -> newProxy(clientInterface, new DriftInvocationHandler(serviceMetadata.getName(), methods, context, headers));
    }
}
