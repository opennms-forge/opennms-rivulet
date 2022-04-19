import org.opennms.netmgt.model.OnmsSeverity;
import org.opennms.features.topology.plugins.topo.graphml.status.GraphMLEdgeStatus;

thickGreen  = [ 'stroke' :  'green', 'stroke-width' :  '3' ];
thickBlue   = [ 'stroke' :   'blue', 'stroke-width' :  '3' ];
thickYellow = [ 'stroke' : 'yellow', 'stroke-width' :  '3' ];

// Defaults
Map<String, String> style = thickGreen;
OnmsSeverity severity = OnmsSeverity.NORMAL;

String direction = edge.getProperties().get("DIRECTION");
if ( direction == "INGRESS" ) {
    style = thickGreen;
} else if ( direction == "EGRESS" ) {
    style = thickBlue;
} else {
    style = thickYellow;
}

return new GraphMLEdgeStatus().severity(severity).style(style);
