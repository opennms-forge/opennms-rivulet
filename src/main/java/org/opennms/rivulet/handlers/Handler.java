/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2019 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2019 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.rivulet.handlers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;

import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.distributed.core.api.Identity;
import org.opennms.netmgt.dnsresolver.api.DnsResolver;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.flows.elastic.FlowDocument;
import org.opennms.netmgt.telemetry.api.receiver.TelemetryMessage;
import org.opennms.netmgt.telemetry.listeners.UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.adapter.common.NetflowMessage;
import org.opennms.netmgt.telemetry.protocols.netflow.transport.FlowMessage;
import org.opennms.rivulet.FakeDispatcher;
import org.opennms.rivulet.FakeDnsResolver;
import org.opennms.rivulet.FakeIdentity;
import org.opennms.rivulet.Rivulet;
import org.opennms.rivulet.StderrEventForwarder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.pkts.PacketHandler;
import io.pkts.packet.Packet;
import io.pkts.packet.UDPPacket;
import io.pkts.protocol.Protocol;

public final class Handler implements PacketHandler {

    private final static Logger LOG = LoggerFactory.getLogger(Handler.class);

    private final UdpParser parser;

    public Handler(final Rivulet rivulet, final HandlerFactory factory) {
        final AsyncDispatcher<TelemetryMessage> dispatcher = new FakeDispatcher(this::handle);
        final EventForwarder eventForwarder = new StderrEventForwarder();
        final Identity identity = new FakeIdentity();
        final MetricRegistry metricRegistry = new MetricRegistry();
        final DnsResolver dnsResolver = new FakeDnsResolver();

        this.parser = Objects.requireNonNull(factory.parser(dispatcher, eventForwarder, identity, dnsResolver, metricRegistry));

        this.parser.start(new DefaultEventExecutor());
    }

    @Override
    public boolean nextPacket(final Packet packet) throws IOException {
        if (packet.hasProtocol(Protocol.UDP)) {
            final UDPPacket udp = (UDPPacket) packet.getPacket(Protocol.UDP);

            final InetSocketAddress remoteAddress = new InetSocketAddress(InetAddressUtils.getInetAddress(udp.getSourceIP()), udp.getSourcePort());
            final InetSocketAddress localAddress = new InetSocketAddress(InetAddressUtils.getInetAddress(udp.getDestinationIP()), udp.getDestinationPort());

            final ByteBuf buffer = Unpooled.wrappedBuffer(udp.getPayload().getArray());
            try {
                this.parser.parse(buffer, remoteAddress, localAddress);
            } catch (final Exception e) {
                LOG.error("Failed to parse packet {}->{}@{}: {}", remoteAddress, localAddress, udp.getArrivalTime(), e.getMessage(), e);
            }
        }
        return true;
    }

    private void handle(final TelemetryMessage telemetryMessage) {
        final FlowMessage flowMessage;
        try {
             flowMessage = FlowMessage.parseFrom(telemetryMessage.getBuffer());
        } catch (InvalidProtocolBufferException ex) {
            LOG.error("Error deserializing flow message", ex);
            return;
        }

        final Gson gson = new GsonBuilder()
                .serializeNulls()
                .create();

        final NetflowMessage netflowMessage = new NetflowMessage(flowMessage, telemetryMessage.getReceivedAt().toInstant());
        final FlowDocument flowDocument = FlowDocument.from(netflowMessage);

        System.out.println(gson.toJson(flowDocument, FlowDocument.class));
    }
}
