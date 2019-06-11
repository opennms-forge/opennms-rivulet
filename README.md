# rivulet - OpenNMS flow analyzer
rivulate is a tool for debugging and analyzing flows and the processing of these using OpenNMS telemetryd.
It reads a `pcap` file and sends the contained packets to a flow parser of telemetryd.
The parsed flow data is then printed to standard output.

## Compiling
rivulet needs a local compiled OpenNMS source tree as it uses the parser from this source tree.
Tha parser packages must be installed into the local maven repository before compiling rivulet.

To compile rivulet, you can use the following command:
```
mvn clean package
```

## Usage
The following cammand can be used to to analyze a `pcap` file:
```
java -jar target/org.opennms.rivulet-1.0-SNAPSHOT-jar-with-dependencies.jar path/to/netflow_v9.pcap Netflow9
```

The first parameter specifies the file to parse and the second parameter specifies the type of flows expected.

rivulet will read all packets from the `pcap` file and try to parse them.
There is no additional filtering before parsing the packet - so please ensure that there are only valid packets in the file.

## Status
At the moment, only Netflow9 is implemented - more support will follow...
