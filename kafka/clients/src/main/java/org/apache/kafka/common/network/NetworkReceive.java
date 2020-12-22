/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(String source) {
        this.source = source;
        //这里就是解决粘包拆包关键
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    // Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
    // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
    // This can go away after we get rid of BlockingChannel
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {
        int read = 0;
        //
        if (size.hasRemaining()) {
            //读取的本质在这，其实就是 SocketChannel.read() ;
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                //rewind将position重置为0，此时就可以从ByteBuffer里读取数据了
                size.rewind();
                //size.getInt() 默认从ByteBuffer当前position的位置获取4个字节，转换为一个int类型的数字返回给你
                // receiveSize 就代表响应消息大小
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");

                //然后分配响应消息大小的一个 buffer ，用这个 buffer 去读取
                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }
        if (buffer != null) {
            //在这里就是读取，这个 buffer 的字节数和下一条的字节数一模一样
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

}
