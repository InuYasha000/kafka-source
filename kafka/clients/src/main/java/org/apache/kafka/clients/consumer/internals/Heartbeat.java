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
package org.apache.kafka.clients.consumer.internals;

/**
 * A helper class for managing the heartbeat to the coordinator
 */
public final class Heartbeat {
    //会话超时时间，超过表示会话失效
    private final long timeout;
    //心跳间隔，表示多久发送一次心跳
    private final long interval;

    //上一次会话重置时间
    private long lastHeartbeatSend;
    //发送心跳请求时，记录发送时间
    private long lastHeartbeatReceive;
    //接收心跳结果后，记录接收时间
    private long lastSessionReset;

    public Heartbeat(long timeout,
                     long interval,
                     long now) {
        if (interval >= timeout)
            throw new IllegalArgumentException("Heartbeat must be set lower than the session timeout");

        this.timeout = timeout;
        this.interval = interval;
        this.lastSessionReset = now;
    }

    public void sentHeartbeat(long now) {
        this.lastHeartbeatSend = now;
    }

    public void receiveHeartbeat(long now) {
        this.lastHeartbeatReceive = now;
    }

    public boolean shouldHeartbeat(long now) {
        return timeToNextHeartbeat(now) == 0;
    }
    
    public long lastHeartbeatSend() {
        return this.lastHeartbeatSend;
    }

    //就是当前时间到下一次调度的时间间隔
    //返回0表示需要立马发送心跳，大于0表示还需要多少秒才需要发送心跳
    public long timeToNextHeartbeat(long now) {
        //上次发送心跳后到现在过去了多长时间
        long timeSinceLastHeartbeat = now - Math.max(lastHeartbeatSend, lastSessionReset);

        //超过心跳间隔
        if (timeSinceLastHeartbeat > interval)
            return 0;
        else
            //多久还要发生一次心跳
            return interval - timeSinceLastHeartbeat;
    }

    public boolean sessionTimeoutExpired(long now) {
        return now - Math.max(lastSessionReset, lastHeartbeatReceive) > timeout;
    }

    public long interval() {
        return interval;
    }

    public void resetSessionTimeout(long now) {
        this.lastSessionReset = now;
    }

}