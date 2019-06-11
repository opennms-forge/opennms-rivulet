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

package org.opennms.rivulet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import org.opennms.netmgt.telemetry.protocols.netflow.parser.Netflow5UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.Netflow9UdpParser;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.session.Session;
import org.opennms.netmgt.telemetry.protocols.netflow.parser.session.UdpSessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.pkts.PacketHandler;
import io.pkts.packet.Packet;
import io.pkts.packet.UDPPacket;
import io.pkts.protocol.Protocol;

public class Netflow9UdpHandler implements PacketHandler {
    private final static Logger LOG = LoggerFactory.getLogger(Netflow9UdpHandler.class);

    private final ScheduledExecutorService executorService = new DefaultEventExecutor();

    private final Netflow9UdpParser parser;

    public Netflow9UdpHandler(final Rivulet rivulet) {
        this.parser = new Netflow9UdpParser("rivulet:netflow9:udp", new OutputDispatcher());
        this.parser.start(this.executorService);
    }

    @Override
    public boolean nextPacket(final Packet packet) throws IOException {
        if (packet.hasProtocol(Protocol.UDP)) {
            final UDPPacket udp = (UDPPacket) packet.getPacket(Protocol.UDP);
            LOG.trace("{}", udp);

            final InetSocketAddress remoteAddress = InetSocketAddress.createUnresolved(udp.getSourceIP(), udp.getSourcePort());
            final InetSocketAddress localAddress = InetSocketAddress.createUnresolved(udp.getDestinationIP(), udp.getDestinationPort());

            final ByteBuffer buffer = ByteBuffer.wrap(udp.getPayload().getArray());

            try {
                this.parser.parse(buffer, remoteAddress, localAddress);
            } catch (final Exception e) {
                LOG.error("Failed to parse packet {}->{}@{}: {}", remoteAddress, localAddress, udp.getArrivalTime(), e.getMessage(), e);
            }
        }
        return true;
    }
}
