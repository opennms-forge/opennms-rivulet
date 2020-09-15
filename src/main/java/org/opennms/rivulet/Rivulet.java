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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.opennms.rivulet.handlers.Handler;
import org.opennms.rivulet.handlers.HandlerFactory;
import org.opennms.rivulet.handlers.IpfixUdpHandlerFactory;
import org.opennms.rivulet.handlers.Netflow5UdpHandlerFactory;
import org.opennms.rivulet.handlers.Netflow9UdpHandlerFactory;

import io.pkts.Pcap;

public class Rivulet {
    public final Path file;
    public final Proto proto;

    public Rivulet(final CmdLine cmdLine) {
        this.file = cmdLine.file;
        this.proto = cmdLine.proto;
    }

    public static void main(final String... args) throws Exception {
        final CmdLine cmdLine = new CmdLine();
        final CmdLineParser parser = new CmdLineParser(cmdLine);
        try {
            parser.parseArgument(args);
        } catch (final CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.err.println();
            System.exit(1);
        }

        final Rivulet app = new Rivulet(cmdLine);
        app.run();
    }

    private void run() throws Exception {
        try (final InputStream in = Files.newInputStream(this.file)) {
            final Pcap pcap = Pcap.openStream(in);

            final HandlerFactory factory;
            switch (this.proto) {
                case Netflow5:
                    factory = new Netflow5UdpHandlerFactory();
                    break;

                case Netflow9:
                    factory = new Netflow9UdpHandlerFactory();
                    break;

                case IPFIX:
                    factory = new IpfixUdpHandlerFactory();
                    break;

                case SFlow:
                    throw new RuntimeException("Not yet implemented");

                default:
                    throw new RuntimeException("unreachable");
            }

            final Handler handler = new Handler(this, factory);
            pcap.loop(handler);
        }
    }

    public enum Proto {
        Netflow5,
        Netflow9,
        IPFIX,
        SFlow,
    }

    public static class CmdLine {
        @Argument(index = 0, metaVar = "FILE", required = true)
        private Path file;

        @Argument(index = 1, metaVar = "PROTO", required = true)
        private Proto proto;
    }
}
