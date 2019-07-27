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
import java.nio.ByteBuffer;
import java.util.Objects;

import org.opennms.core.ipc.sink.api.AsyncDispatcher;
import org.opennms.core.utils.InetAddressUtils;
import org.opennms.distributed.core.api.Identity;
import org.opennms.netmgt.events.api.EventForwarder;
import org.opennms.netmgt.flows.api.FlowRepository;
import org.opennms.netmgt.telemetry.api.adapter.Adapter;
import org.opennms.netmgt.telemetry.api.adapter.TelemetryMessageLog;
import org.opennms.netmgt.telemetry.api.receiver.TelemetryMessage;
import org.opennms.netmgt.telemetry.listeners.UdpParser;
import org.opennms.rivulet.FakeDispatcher;
import org.opennms.rivulet.FakeFlowRepository;
import org.opennms.rivulet.FakeIdentity;
import org.opennms.rivulet.FakeTelemetryMessageLog;
import org.opennms.rivulet.Rivulet;
import org.opennms.rivulet.StderrEventForwarder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.pkts.PacketHandler;
import io.pkts.packet.Packet;
import io.pkts.packet.UDPPacket;
import io.pkts.protocol.Protocol;

public final class Handler implements PacketHandler {

    private final static Logger LOG = LoggerFactory.getLogger(Handler.class);

    private final UdpParser parser;
    private final Adapter adapter;

    public Handler(final Rivulet rivulet, final HandlerFactory factory) {
        final AsyncDispatcher<TelemetryMessage> dispatcher = new FakeDispatcher(this::handle, rivulet.logTransport);
        final EventForwarder eventForwarder = new StderrEventForwarder();
        final Identity identity = new FakeIdentity();
        this.parser = Objects.requireNonNull(factory.parser(dispatcher, eventForwarder, identity));

        final MetricRegistry metricRegistry = new MetricRegistry();
        final FlowRepository flowRepository = new FakeFlowRepository();
        this.adapter = Objects.requireNonNull(factory.adapter(metricRegistry, flowRepository));

        this.parser.start(new DefaultEventExecutor());
    }

    @Override
    public boolean nextPacket(final Packet packet) throws IOException {
        if (packet.hasProtocol(Protocol.UDP)) {
            final UDPPacket udp = (UDPPacket) packet.getPacket(Protocol.UDP);

            final InetSocketAddress remoteAddress = new InetSocketAddress(InetAddressUtils.getInetAddress(udp.getSourceIP()), udp.getSourcePort());
            final InetSocketAddress localAddress = new InetSocketAddress(InetAddressUtils.getInetAddress(udp.getDestinationIP()), udp.getDestinationPort());

            final ByteBuffer buffer = ByteBuffer.wrap(udp.getPayload().getArray());

            try {
                this.parser.parse(buffer, remoteAddress, localAddress);
            } catch (final Exception e) {
                LOG.error("Failed to parse packet {}->{}@{}: {}", remoteAddress, localAddress, udp.getArrivalTime(), e.getMessage(), e);
            }
        }
        return true;
    }

    private void handle(final TelemetryMessage telemetryMessage) {
        final TelemetryMessageLog messageLog = new FakeTelemetryMessageLog(
                InetAddressUtils.toIpAddrString(telemetryMessage.getSource().getAddress()),
                telemetryMessage.getSource().getPort(),
                telemetryMessage.getReceivedAt().getTime(),
                telemetryMessage.getBuffer().array());

        this.adapter.handleMessageLog(messageLog);
    }
}
