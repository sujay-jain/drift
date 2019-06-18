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
package com.facebook.drift.transport.netty.codec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.facebook.drift.transport.netty.codec.Transport.HEADER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.builder;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class HeaderTransport
{
    private static final Logger log = Logger.get(HeaderTransport.class);
    private static final int HEADER_MAGIC = 0x0FFF;
    private static final int FRAME_HEADER_SIZE =
            Short.BYTES +               // magic
                    Short.BYTES +       // flags
                    Integer.BYTES +     // sequenceId
                    Short.BYTES;        // header size

    private static final int FLAGS_NONE = 0;
    private static final int FLAG_SUPPORT_OUT_OF_ORDER = 1;
    private static final int FLAG_SUPPORT_OUT_OF_ORDER_MASK = 1;

    private static final int NORMAL_HEADERS = 1;
    private static final int PERSISTENT_HEADERS = 1;
    private static final int MAX_MESSAGE_BEGIN_SECTION_SIZE = 1048576; // 1MB at most for message name + sequence Id + length

    private HeaderTransport() {}

    /**
     * Encodes the HeaderFrame into a ByteBuf transferring the reference ownership.
     *
     * @param bufAllocator
     * @param frame frame to be encoded; reference count ownership is transferred to this method
     * @return the encoded frame data; caller is responsible for releasing this buffer
     */
    public static ByteBuf encodeFrame(ByteBufAllocator bufAllocator, ThriftFrame frame)
    {
        try {
            // describe the encoding (Thrift protocol, compression info)
            ByteBuf encodingInfo = Unpooled.buffer(3);
            encodingInfo.writeByte(frame.getProtocol().getHeaderTransportId());
            List<ThriftHeaderTransform> writeTransforms = frame.getTransforms();
            writeVint(encodingInfo, writeTransforms.size());
            for (ThriftHeaderTransform transform : writeTransforms) {
                writeVint(encodingInfo, transform.getId());
            }
            // headers
            ByteBuf encodedHeaders = encodeHeaders(frame.getHeaders());

            // Padding - header size must be a multiple of 4
            int headerSize = encodingInfo.readableBytes() + encodedHeaders.readableBytes();
            ByteBuf padding = getPadding(headerSize);
            headerSize += padding.readableBytes();

            // frame header (magic, flags, sequenceId, headerSize
            ByteBuf frameHeader = Unpooled.buffer(FRAME_HEADER_SIZE);
            frameHeader.writeShort(HEADER_MAGIC);
            frameHeader.writeShort(frame.isSupportOutOfOrderResponse() ? FLAG_SUPPORT_OUT_OF_ORDER : FLAGS_NONE);
            frameHeader.writeInt(frame.getSequenceId());
            frameHeader.writeShort(headerSize >> 2);

            // header frame is a simple wrapper around the frame method, so the frame does not need to be released
            return Unpooled.wrappedBuffer(
                    frameHeader,
                    encodingInfo,
                    encodedHeaders,
                    padding,
                    transform(bufAllocator, frame.getMessage(), writeTransforms));
        }
        finally {
            frame.release();
        }
    }

    private static ByteBuf getPadding(int headerSize)
    {
        int paddingSize = 4 - headerSize % 4;
        ByteBuf padding = Unpooled.buffer(paddingSize);
        padding.writeZero(paddingSize);
        return padding;
    }

    private static ByteBuf encodeHeaders(Map<String, String> headers)
    {
        if (headers.isEmpty()) {
            return Unpooled.EMPTY_BUFFER;
        }

        // 1 bytes for header type, 5 for header count vint, and 5 for each header key and value length vint
        int estimatedSize = 1 + 5 + (headers.size() * 10);
        for (Entry<String, String> entry : headers.entrySet()) {
            // assume the key and value are ASCII
            estimatedSize += entry.getKey().length() + entry.getValue().length();
        }

        ByteBuf headersBuffer = Unpooled.buffer(estimatedSize);
        // non persistent header
        headersBuffer.writeByte(0x01);
        writeVint(headersBuffer, headers.size());
        for (Entry<String, String> entry : headers.entrySet()) {
            writeString(headersBuffer, entry.getKey());
            writeString(headersBuffer, entry.getValue());
        }
        return headersBuffer;
    }

    private static void writeString(ByteBuf out, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        writeVint(out, bytes.length);
        out.writeBytes(bytes);
    }

    private static void writeVint(ByteBuf out, int n)
    {
        while (true) {
            if ((n & ~0x7F) == 0) {
                out.writeByte(n);
                return;
            }

            out.writeByte(n | 0x80);
            n >>>= 7;
        }
    }

    /**
     * Decodes the ByteBuf into a HeaderFrame transferring the reference ownership.
     *
     * @param bufAllocator allocator to allocated byteBuf to contain decoded result.
     * @param buffer buffer to be decoded; reference count ownership is transferred to this method
     * @return the decoded frame; caller is responsible for releasing this object
     */
    public static ThriftFrame decodeFrame(ByteBufAllocator bufAllocator, ByteBuf buffer)
    {
        ByteBuf messageHeader = null;
        try {
            // frame header
            short magic = buffer.readShort();
            verify(magic == HEADER_MAGIC, "Invalid header magic");
            short flags = buffer.readShort();
            boolean outOfOrderResponse;
            switch (flags) {
                case FLAGS_NONE:
                    outOfOrderResponse = false;
                    break;
                case FLAG_SUPPORT_OUT_OF_ORDER:
                    outOfOrderResponse = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported header flags: " + flags);
            }
            int frameSequenceId = buffer.readInt();
            int headerSize = buffer.readShort() << 2;
            messageHeader = buffer.readBytes(headerSize);

            // encoding info
            byte protocolId = messageHeader.readByte();
            Protocol protocol = Protocol.getProtocolByHeaderTransportId(protocolId);
            int numberOfTransforms = readVariableLengthInt(messageHeader);
            checkArgument(numberOfTransforms >= 0, "invalid number of transforms: %s", numberOfTransforms);
            ImmutableList.Builder<ThriftHeaderTransform> readTransformsBuilder = ImmutableList.builder();

            for (int i = 0; i < numberOfTransforms; i++) {
                int transformId = readVariableLengthInt(messageHeader);
                readTransformsBuilder.add(ThriftHeaderTransform.fromId(transformId));
            }
            List<ThriftHeaderTransform> readTransforms = readTransformsBuilder.build();

            // headers
            // todo what about duplicate headers?
            ImmutableMap.Builder<String, String> allHeaders = ImmutableMap.builder();
            allHeaders.putAll(decodeHeaders(NORMAL_HEADERS, messageHeader));
            allHeaders.putAll(decodeHeaders(PERSISTENT_HEADERS, messageHeader));

            // message
            ByteBuf message = untransform(bufAllocator, buffer.retainedDuplicate(), readTransforms);

            // header frame wraps message byte buffer, so message should not be release yet
            return new ThriftFrame(frameSequenceId, message, allHeaders.build(), readTransforms, HEADER, protocol, outOfOrderResponse);
        }
        finally {
            // message header in an independent buffer and must be released
            if (messageHeader != null) {
                messageHeader.release();
            }

            // input buffer has been consumed and transformed into a HeaderFrame, so release it
            buffer.release();
        }
    }

    private static ByteBuf transform(ByteBufAllocator bufAllocator, ByteBuf data, List<ThriftHeaderTransform> writeTransforms)
    {
        for (ThriftHeaderTransform transform : writeTransforms) {
            data = transform.getMessageTransformer().transform(bufAllocator, data);
        }
        return data;
    }

    private static ByteBuf untransform(ByteBufAllocator bufAllocator, ByteBuf data, List<ThriftHeaderTransform> readTransforms)
    {
        for (ThriftHeaderTransform transform : readTransforms) {
            data = transform.getMessageTransformer().untransform(bufAllocator, data);
        }
        return data;
    }

    private static Map<String, String> decodeHeaders(int expectedHeadersType, ByteBuf messageHeader)
    {
        if (messageHeader.readableBytes() == 0) {
            return ImmutableMap.of();
        }

        byte headersType = messageHeader.readByte();
        if (headersType != expectedHeadersType) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
        int headerCount = readVariableLengthInt(messageHeader);
        for (int i = 0; i < headerCount; i++) {
            String key = readString(messageHeader);
            String value = readString(messageHeader);
            headers.put(key, value);
        }
        return headers.build();
    }

    private static String readString(ByteBuf messageHeader)
    {
        int length = readVariableLengthInt(messageHeader);
        return messageHeader.readCharSequence(length, UTF_8).toString();
    }

    private static int readVariableLengthInt(ByteBuf messageHeader)
    {
        int result = 0;
        int shift = 0;

        while (true) {
            byte b = messageHeader.readByte();
            result |= (b & 0x7f) << shift;
            if ((b & 0x80) != 0x80) {
                break;
            }
            shift += 7;
        }

        return result;
    }

    public static Optional<FrameInfo> tryDecodeFrameInfo(ByteBufAllocator bufAllocator, ByteBuf input)
    {
        ByteBuf buffer = input.retainedDuplicate();
        ByteBuf messageHeader = null;
        try {
            if (buffer.readableBytes() < FRAME_HEADER_SIZE) {
                return Optional.empty();
            }
            // skip magic
            buffer.readShort();
            short flags = buffer.readShort();
            boolean outOfOrderResponse = (flags & FLAG_SUPPORT_OUT_OF_ORDER_MASK) == 1;
            int headerSequenceId = buffer.readInt();
            int headerSize = buffer.readShort() << 2;

            if (buffer.readableBytes() < headerSize) {
                return Optional.empty();
            }

            messageHeader = buffer.readBytes(headerSize);
            byte protocolId = messageHeader.readByte();
            Protocol protocol = Protocol.getProtocolByHeaderTransportId(protocolId);

            int numberOfTransforms = readVariableLengthInt(messageHeader);
            checkArgument(numberOfTransforms >= 0, "invalid number of transforms: %s", numberOfTransforms);
            Builder<ThriftHeaderTransform> readTransformBuilder = builder();

            for (int i = 0; i < numberOfTransforms; i++) {
                int transformId = readVariableLengthInt(messageHeader);
                readTransformBuilder.add(ThriftHeaderTransform.fromId(transformId));
            }
            if (numberOfTransforms > 0) {
                List<ThriftHeaderTransform> transforms = readTransformBuilder.build();
                for (ThriftHeaderTransform transform : transforms) {
                    buffer = transform.getMessageTransformer().tryUntransform(bufAllocator, buffer, MAX_MESSAGE_BEGIN_SECTION_SIZE);
                }
            }
            SimpleFrameInfoDecoder simpleFrameInfoDecoder = new SimpleFrameInfoDecoder(HEADER, protocol, outOfOrderResponse);
            Optional<FrameInfo> frameInfo = simpleFrameInfoDecoder.tryDecodeFrameInfo(bufAllocator, buffer);
            if (frameInfo.isPresent()) {
                int messageSequenceId = frameInfo.get().getSequenceId();
                checkArgument(
                        headerSequenceId == messageSequenceId,
                        "Sequence ids don't match. headerSequenceId: %s. messageSequenceId: %s",
                        headerSequenceId,
                        messageSequenceId);
            }
            return frameInfo;
        }
        finally {
            if (messageHeader != null) {
                messageHeader.release();
            }
            buffer.release();
        }
    }
}
