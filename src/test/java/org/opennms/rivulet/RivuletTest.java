package org.opennms.rivulet;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.io.FileUtils;
import org.graphdrawing.graphml.GraphmlType;
import org.graphdrawing.graphml.KeyForType;
import org.graphdrawing.graphml.KeyType;
import org.graphdrawing.graphml.KeyTypeType;
import org.junit.Before;
import org.junit.Test;
import org.opennms.features.graphml.model.GraphML;
import org.opennms.features.graphml.model.GraphMLEdge;
import org.opennms.features.graphml.model.GraphMLGraph;
import org.opennms.features.graphml.model.GraphMLNode;
import org.opennms.features.graphml.model.GraphMLWriter;
import org.opennms.features.graphml.model.InvalidGraphException;
import org.opennms.features.topology.plugins.topo.graphml.GraphMLProperties;
import org.opennms.netmgt.flows.elastic.Direction;
import org.opennms.netmgt.flows.elastic.FlowDocument;

public class RivuletTest {

    static class Icons {
        public static String Switch = "switch";
    }

    long start, end;

    @Before
    public void setUp() {
        String startDate = "Mon, 18 Apr 2022 10:43:29 -0400";
        String endDate = "Mon, 18 Apr 2022 11:03:36 -0400";

        DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
        ZonedDateTime startTime = ZonedDateTime.parse(startDate, formatter);
        ZonedDateTime endTime = ZonedDateTime.parse(endDate, formatter);
        System.out.println("Start: " + startTime);
        System.out.println("End: " + endTime);
        start = startTime.toInstant().toEpochMilli();
        end = endTime.toInstant().toEpochMilli();
        System.out.println("Start: " + start);
        System.out.println("End: " + end);
    }

    private List<FlowDocument> getFlows() throws Exception {
        Rivulet.CmdLine cmdLine = new Rivulet.CmdLine();
        cmdLine.file = new File("/tmp/flows.pcap").toPath();
        cmdLine.proto = Rivulet.Proto.Netflow9;

        List<FlowDocument> flows = new ArrayList<>();
        Rivulet rivulet = new Rivulet(cmdLine) {
            @Override
            public void onFlowDocument(FlowDocument flowDocument) {
                if (flowDocument == null) {
                    return;
                }
                flows.add(flowDocument);
            }
        };
        rivulet.run();

        System.out.println("Total flows in capture: " + flows.size());


        Set<Integer> inputSnmp = new LinkedHashSet<>();
        Set<Integer> outputSnmp = new LinkedHashSet<>();
        Set<String> vlans = new LinkedHashSet<>();
        long totalBytes = 0;

        List<FlowDocument> matchingFlows = new ArrayList<>();
        for (FlowDocument flowDocument : flows) {
            if ( flowDocument.getLastSwitched() > start && flowDocument.getDeltaSwitched() < end) {
                matchingFlows.add(flowDocument);
                totalBytes += flowDocument.getBytes();

                inputSnmp.add(flowDocument.getInputSnmp());
                outputSnmp.add(flowDocument.getOutputSnmp());
                vlans.add(flowDocument.getVlan());
            }
        }
        System.out.println("Total flows in time range: " + matchingFlows.size());
        System.out.println("Total bytes from flows: " + FileUtils.byteCountToDisplaySize(totalBytes)); // 167.934455871582031
        System.out.println("Input SNMP: " + inputSnmp);
        System.out.println("Output SNMP: " + outputSnmp);
        System.out.println("VLANs: " + vlans);
        return matchingFlows;
    }

    private static class FlowStats {
        int inputInterface;
        int outputInterface;
        Direction direction;
        Set<String> vlans = new LinkedHashSet<>();
        long bytes = 0;

        public FlowStats(Integer inputInterface, Integer outputInterface, Direction direction) {
            this.inputInterface = inputInterface;
            this.outputInterface = outputInterface;
            this.direction = direction;
        }

        public void addVlan(String vlan) {
            vlans.add(vlan);
        }

        public String getKey() {
            return String.format("%d:%d:%s", inputInterface, outputInterface, direction);
        }
    }

    @Test
    public void canGenerateGraph() throws Exception {
        List<FlowDocument> flows = getFlows();
        Set<Integer> intfs = new LinkedHashSet<>();
        Map<String, FlowStats> flowStats = new LinkedHashMap<>();
        for (FlowDocument flow : flows) {
            intfs.add(flow.getInputSnmp());
            intfs.add(flow.getOutputSnmp());
            FlowStats stats = new FlowStats(flow.getInputSnmp(), flow.getOutputSnmp(), flow.getDirection());
            if (flowStats.containsKey(stats.getKey())) {
                stats = flowStats.get(stats.getKey());
            } else {
                flowStats.put(stats.getKey(), stats);
            }
            stats.addVlan(flow.getVlan());
            stats.bytes += flow.getBytes();
        }

        final GraphML graphML = new GraphML();
        graphML.setProperty(GraphMLProperties.LABEL, "Netflow Graph");
        graphML.setProperty(GraphMLProperties.BREADCRUMB_STRATEGY, "SHORTEST_PATH_TO_ROOT");

        // Generate the OVERVIEW GRAPH
        final GraphMLGraph overviewGraph = new GraphMLGraph();
        overviewGraph.setProperty(GraphMLProperties.ID, "netflow");
        overviewGraph.setProperty(GraphMLProperties.LABEL, "Netflow");
        overviewGraph.setProperty(GraphMLProperties.DESCRIPTION, "Mo flows mo problems");
        overviewGraph.setProperty(GraphMLProperties.NAMESPACE, "netflow:overview");
        for (Integer intf : intfs) {
            GraphMLNode node = createNode(Integer.toString(intf));
            node.setProperty(GraphMLProperties.ICON_KEY, Icons.Switch);

            long totalBytesIn = 0, totalBytesOut = 0;
            for (FlowDocument flow : flows) {
                if (flow.getInputSnmp().equals(intf) && flow.getDirection().equals(Direction.INGRESS)) {
                    totalBytesIn += flow.getBytes();
                } else if (flow.getOutputSnmp().equals(intf) && flow.getDirection().equals(Direction.EGRESS)) {
                    totalBytesOut += flow.getBytes();
                }
            }
            node.setProperty("BYTES_IN", totalBytesIn);
            node.setProperty("DISPLAY_BYTES_IN", FileUtils.byteCountToDisplaySize(totalBytesIn));
            node.setProperty("BYTES_OUT", totalBytesOut);
            node.setProperty("DISPLAY_BYTES_OUT", FileUtils.byteCountToDisplaySize(totalBytesOut));

            overviewGraph.addNode(node);
        }
        graphML.addGraph(overviewGraph);

        for (FlowStats flowStat : flowStats.values()) {
            GraphMLEdge edge = new GraphMLEdge();
            edge.setSource(overviewGraph.getNodeById(Integer.toString(flowStat.inputInterface)));
            edge.setTarget(overviewGraph.getNodeById(Integer.toString(flowStat.outputInterface)));
            edge.setProperty(GraphMLProperties.ID, flowStat.getKey());
            edge.setProperty("INPUT_INTERFACE", flowStat.inputInterface);
            edge.setProperty("OUTPUT_INTERFACE", flowStat.outputInterface);
            edge.setProperty("BYTES", flowStat.bytes);
            edge.setProperty("DISPLAY_BYTES", FileUtils.byteCountToDisplaySize(flowStat.bytes));
            edge.setProperty("DIRECTION", flowStat.direction.toString());
            edge.setProperty("VLAN", flowStat.vlans.toString());
            overviewGraph.addEdge(edge);
        }

        // Regions: Add all of the interfaces to the default focus
        overviewGraph.setProperty(GraphMLProperties.FOCUS_STRATEGY, "ALL");
        overviewGraph.setProperty(GraphMLProperties.PREFERRED_LAYOUT, "D3 Layout");

        // Use a default SZL of 0 across all graphs
        for (GraphMLGraph graph : graphML.getGraphs()) {
            graph.setProperty(GraphMLProperties.SEMANTIC_ZOOM_LEVEL, 0);
        }

        storeToDisk(graphML, "/tmp/graph.xml");
    }

    public static void storeToDisk(GraphML graphML, String filename) throws InvalidGraphException, IOException {
        // Save, and set namespace
        GraphMLWriter.write(graphML, new File(filename), new GraphMLWriter.ProcessHook() {
            @Override
            public void process(GraphML input, GraphmlType result) {
                KeyType keyType = new KeyType();
                keyType.setFor(KeyForType.ALL);
                keyType.setId(GraphMLProperties.NAMESPACE);
                keyType.setAttrType(KeyTypeType.STRING);
                keyType.setAttrName(GraphMLProperties.NAMESPACE);

                result.getKey().add(keyType);
            }
        });
    }

    @Test
    public void doIt() throws Exception {
        Rivulet.CmdLine cmdLine = new Rivulet.CmdLine();
        cmdLine.file = new File("flows.pcap").toPath();
        cmdLine.proto = Rivulet.Proto.Netflow9;

        List<FlowDocument> flows = new ArrayList<>();
        Rivulet rivulet = new Rivulet(cmdLine) {
            @Override
            public void onFlowDocument(FlowDocument flowDocument) {
                if (flowDocument == null) {
                    return;
                }
                flows.add(flowDocument);
            }
        };
        rivulet.run();

        System.out.println("Total flows in capture: " + flows.size());


        Set<Integer> inputSnmp = new LinkedHashSet<>();
        Set<Integer> outputSnmp = new LinkedHashSet<>();
        Set<String> vlans = new LinkedHashSet<>();
        long totalBytes = 0;

        List<FlowDocument> matchingFlows = new ArrayList<>();
        for (FlowDocument flowDocument : flows) {
            if ( flowDocument.getLastSwitched() > start && flowDocument.getDeltaSwitched() < end) {
                matchingFlows.add(flowDocument);
                totalBytes += flowDocument.getBytes();

                inputSnmp.add(flowDocument.getInputSnmp());
                outputSnmp.add(flowDocument.getOutputSnmp());
                vlans.add(flowDocument.getVlan());
            }
        }
        System.out.println("Total flows in time range: " + matchingFlows.size());
        System.out.println("Total bytes from flows: " + FileUtils.byteCountToDisplaySize(totalBytes));
        System.out.println("Input SNMP: " + inputSnmp);
        System.out.println("Output SNMP: " + outputSnmp);
        System.out.println("VLANs: " + vlans);

        printFlowSummary(matchingFlows, f -> f.getInputSnmp() == 57 && f.getDirection().equals(Direction.INGRESS));
        //printFlowSummary(matchingFlows, f -> f.getInputSnmp() == 57 && f.getDirection().equals(Direction.EGRESS));
    }

    private void printFlowSummary(List<FlowDocument> flows, Predicate<FlowDocument> predicate) {
        System.out.println("-------");
        List<FlowDocument> matchingFlows = new ArrayList<>();
        long totalBytes = 0;
        Set<Integer> inputSnmp = new LinkedHashSet<>();
        Set<Integer> outputSnmp = new LinkedHashSet<>();
        Set<String> vlans = new LinkedHashSet<>();
        Set<Direction> directions = new LinkedHashSet<>();

        for (FlowDocument flowDocument : flows) {
            if (predicate.test(flowDocument)) {
                matchingFlows.add(flowDocument);
                totalBytes += flowDocument.getBytes();

                inputSnmp.add(flowDocument.getInputSnmp());
                outputSnmp.add(flowDocument.getOutputSnmp());
                vlans.add(flowDocument.getVlan());

                directions.add(flowDocument.getDirection());
            }
        }
        System.out.println("Matching flows: " + matchingFlows.size());
        System.out.println("Total bytes from flows: " + FileUtils.byteCountToDisplaySize(totalBytes));
        System.out.println("Input SNMP: " + inputSnmp);
        System.out.println("Output SNMP: " + outputSnmp);
        System.out.println("VLANs: " + vlans);
        System.out.println("Directions: " + directions);
    }

    private static GraphMLNode createNode(String id) {
        GraphMLNode node = new GraphMLNode();
        node.setProperty(GraphMLProperties.ID, id);
        node.setProperty(GraphMLProperties.LABEL, "ifIndex: " + id);
        return node;
    }
}
