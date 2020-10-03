/*
 * Copyright (C) 2020 Facebook, Inc.
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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.codec.metadata.ThriftCatalog;
import com.facebook.drift.codec.metadata.ThriftStructMetadata;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.codec.utils.LocaleToLanguageTagCodec;
import com.facebook.drift.codec.utils.UuidToLeachSalzBinaryEncodingThriftCodec;
import com.facebook.drift.protocol.TBinaryProtocol;
import com.facebook.drift.protocol.TCompactProtocol;
import com.facebook.drift.protocol.TFacebookCompactProtocol;
import com.facebook.drift.protocol.TMemoryBuffer;
import com.facebook.drift.protocol.TProtocol;
import com.facebook.drift.protocol.TTransport;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestDefaultThriftCodecsModule
{
    private ThriftCatalog thriftCatalog;
    private ThriftCodecManager codecManager;

    @BeforeTest
    public void setup()
    {
        thriftCatalog = new ThriftCatalog();
        codecManager = new ThriftCodecManager(thriftCatalog);
    }

    @Test
    public void testDataSizeCodec()
            throws Exception
    {
        DataSizeToBytesThriftCodec codec = new DataSizeToBytesThriftCodec(thriftCatalog);
        codecManager.addCodec(codec);

        testRoundTripSerialize(codecManager, new DataSizeStruct(new DataSize(100, BYTE)));
    }

    @Test
    public void testDateTimeCodec()
            throws Exception
    {
        JodaDateTimeToEpochMillisThriftCodec codec = new JodaDateTimeToEpochMillisThriftCodec(thriftCatalog);
        codecManager.addCodec(codec);

        testRoundTripSerialize(codecManager, new DateTimeStruct(new DateTime(1000)));
    }

    @Test
    public void testDurationCodec()
            throws Exception
    {
        DurationToMillisThriftCodec codec = new DurationToMillisThriftCodec(thriftCatalog);
        codecManager.addCodec(codec);

        testRoundTripSerialize(codecManager, new DurationStruct(new Duration(1000, MILLISECONDS)));
    }

    @Test
    public void testLocaleCodec()
            throws Exception
    {
        LocaleToLanguageTagCodec codec = new LocaleToLanguageTagCodec(thriftCatalog);
        codecManager.addCodec(codec);

        testRoundTripSerialize(codecManager, new LocaleStruct(Locale.US));
    }

    @Test
    public void testUuidCodec()
            throws Exception
    {
        UuidToLeachSalzBinaryEncodingThriftCodec codec = new UuidToLeachSalzBinaryEncodingThriftCodec(thriftCatalog);
        codecManager.addCodec(codec);

        testRoundTripSerialize(codecManager, new UuidStruct(UUID.randomUUID()));
    }

    private <T> void testRoundTripSerialize(ThriftCodecManager codecManager, T value)
            throws Exception
    {
        testRoundTripSerialize(codecManager, value, x -> {});
    }

    private <T> void testRoundTripSerialize(ThriftCodecManager codecManager, T value, Consumer<T> consumer)
            throws Exception
    {
        consumer.accept(testRoundTripSerialize(codecManager, value, TBinaryProtocol::new));
        consumer.accept(testRoundTripSerialize(codecManager, value, TCompactProtocol::new));
        consumer.accept(testRoundTripSerialize(codecManager, value, TFacebookCompactProtocol::new));
    }

    private <T> T testRoundTripSerialize(ThriftCodecManager codecManager, T value, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        ThriftCodec<T> codec = (ThriftCodec<T>) codecManager.getCodec(value.getClass());

        return testRoundTripSerialize(codecManager, codec, value, protocolFactory);
    }

    private <T> T testRoundTripSerialize(ThriftCodecManager codecManager, ThriftCodec<T> codec, T structInstance, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        Class<T> structClass = (Class<T>) structInstance.getClass();
        ThriftCatalog readCatalog = codecManager.getCatalog();
        ThriftStructMetadata readMetadata = readCatalog.getThriftStructMetadata(structClass);
        assertNotNull(readMetadata);

        ThriftCatalog writeCatalog = codecManager.getCatalog();
        ThriftStructMetadata writeMetadata = writeCatalog.getThriftStructMetadata(structClass);
        assertNotNull(writeMetadata);

        TMemoryBuffer transport = new TMemoryBuffer(10 * 1024);
        TProtocol protocol = protocolFactory.apply(transport);
        codec.write(structInstance, protocol);

        T copy = codec.read(protocol);
        assertNotNull(copy);
        assertEquals(copy, structInstance);

        return copy;
    }

    @ThriftStruct
    public static class DataSizeStruct
    {
        private DataSize theDataSize;

        @ThriftConstructor
        public DataSizeStruct(DataSize theDataSize)
        {
            this.theDataSize = requireNonNull(theDataSize, "theDataSize is null");
        }

        @ThriftField(1)
        public DataSize getTheDataSize()
        {
            return theDataSize;
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
            DataSizeStruct that = (DataSizeStruct) o;
            return theDataSize.equals(that.theDataSize);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(theDataSize);
        }
    }

    @ThriftStruct
    public static class DateTimeStruct
    {
        private DateTime theDateTime;

        @ThriftConstructor
        public DateTimeStruct(DateTime theDateTime)
        {
            this.theDateTime = requireNonNull(theDateTime, "theDateTime is null");
        }

        @ThriftField(1)
        public DateTime getTheDateTime()
        {
            return theDateTime;
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
            DateTimeStruct that = (DateTimeStruct) o;
            return theDateTime.equals(that.theDateTime);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(theDateTime);
        }
    }

    @ThriftStruct
    public static class DurationStruct
    {
        private Duration theDuration;

        @ThriftConstructor
        public DurationStruct(Duration theDuration)
        {
            this.theDuration = requireNonNull(theDuration, "theDuration is null");
        }

        @ThriftField(1)
        public Duration getTheDuration()
        {
            return theDuration;
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
            DurationStruct that = (DurationStruct) o;
            return theDuration.equals(that.theDuration);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(theDuration);
        }
    }

    @ThriftStruct
    public static class LocaleStruct
    {
        private Locale theLocale;

        @ThriftConstructor
        public LocaleStruct(Locale theLocale)
        {
            this.theLocale = requireNonNull(theLocale, "theLocale is null");
        }

        @ThriftField(1)
        public Locale getTheLocale()
        {
            return theLocale;
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
            LocaleStruct that = (LocaleStruct) o;
            return theLocale.equals(that.theLocale);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(theLocale);
        }
    }

    @ThriftStruct
    public static class UuidStruct
    {
        private UUID theUuid;

        @ThriftConstructor
        public UuidStruct(UUID theUuid)
        {
            this.theUuid = requireNonNull(theUuid, "theUuid is null");
        }

        @ThriftField(1)
        public UUID getTheUuid()
        {
            return theUuid;
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
            UuidStruct that = (UuidStruct) o;
            return theUuid.equals(that.theUuid);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(theUuid);
        }
    }
}
