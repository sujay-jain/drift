/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.swift.codec.guice;

import com.facebook.swift.codec.BonkConstructor;
import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.internal.TProtocolReader;
import com.facebook.swift.codec.internal.TProtocolWriter;
import com.facebook.swift.codec.metadata.ThriftType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.facebook.swift.codec.guice.ThriftCodecBinder.thriftServerBinder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestThriftCodecModule
{
    @Test
    public void testThriftClientAndServerModules()
            throws Exception
    {
    Injector injector = Guice.createInjector(Stage.PRODUCTION,
            new ThriftCodecModule(),
            new Module()
            {
                @Override
                public void configure(Binder binder)
                {
                    thriftServerBinder(binder).bindThriftCodec(BonkConstructor.class);
                        thriftServerBinder(binder).bindListThriftCodec(BonkConstructor.class);
                        thriftServerBinder(binder).bindMapThriftCodec(String.class, BonkConstructor.class);

                        thriftServerBinder(binder).bindThriftCodec(new TypeLiteral<Map<Integer, List<String>>>() {});

                        thriftServerBinder(binder).bindThriftCodec(new ThriftCodec<ValueClass>() {
                            @Override
                            public ThriftType getType()
                            {
                                return new ThriftType(ThriftType.STRING, ValueClass.class);
                            }

                            @Override
                            public ValueClass read(TProtocolReader protocol)
                                    throws Exception
                            {
                                return new ValueClass(protocol.readString());
                            }

                            @Override
                            public void write(ValueClass value, TProtocolWriter protocol)
                                    throws Exception
                            {
                                protocol.writeString(value.getValue());
                            }
                        });
                    }
                });

        testRoundTripSerialize(injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<BonkConstructor>>() {})),
                new BonkConstructor("message", 42));

        testRoundTripSerialize(injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<List<BonkConstructor>>>() {})),
                ImmutableList.of(new BonkConstructor("one", 1), new BonkConstructor("two", 2)));

        testRoundTripSerialize(injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<Map<String, BonkConstructor>>>() {})),
                ImmutableMap.of("uno", new BonkConstructor("one", 1), "dos", new BonkConstructor("two", 2)));

        testRoundTripSerialize(injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<Map<Integer, List<String>>>>() {})),
                ImmutableMap.<Integer, List<String>>of(1, ImmutableList.of("one", "uno"), 2, ImmutableList.of("two", "dos")));

        testRoundTripSerialize(injector.getInstance(Key.get(new TypeLiteral<ThriftCodec<ValueClass>>() {})),
                new ValueClass("my value"));
    }

    public static <T> void testRoundTripSerialize(ThriftCodec<T> codec, T value)
            throws Exception
    {
        // write value
        TMemoryBuffer transport = new TMemoryBuffer(10 * 1024);
        TCompactProtocol protocol = new TCompactProtocol(transport);
        TProtocolWriter protocolWriter = new TProtocolWriter(protocol);
        codec.write(value, protocolWriter);

        // read value back
        TProtocolReader protocolReader = new TProtocolReader(protocol);
        T copy = codec.read(protocolReader);
        assertNotNull(copy);

        // verify they are the same
        assertEquals(copy, value);
    }

    public static class ValueClass
    {
        private final String value;

        public ValueClass(String value)
        {
            Preconditions.checkNotNull(value, "value is null");
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ValueClass that = (ValueClass) o;

            if (!value.equals(that.value)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return value.hashCode();
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("ValueClass");
            sb.append("{value='").append(value).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
}