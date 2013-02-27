package br.ufu.facom.network.dts.floodlight.integration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProvider;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.staticflowentry.IStaticFlowEntryPusher;
import net.floodlightcontroller.storage.IStorageSource;
import net.floodlightcontroller.storage.IStorageSourceListener;

import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.net.www.protocol.http.InMemoryCookieStore;

/**
 * Keep track of source MAC addresses and log when we see new ones.
 */
public class Module implements IOFMessageListener {
	/**
	 * The DTS Agent, and DL-Ontology Parser
	 */
	
	/**
	 * Utils 
	 */
    protected static Logger logger = LoggerFactory.getLogger(DtsModule.class);
    protected IFloodlightProvider floodlightProvider;
    protected IStorageSource storageSource;
    protected IStaticFlowEntryPusher staticFlowEntryPusher;

    public void setFloodlightProvider(IFloodlightProvider floodlightProvider) {
        this.floodlightProvider = floodlightProvider;
    }

    public void startUp() {
    	 floodlightProvider.addOFMessageListener(OFType.HELLO, this);
    	 floodlightProvider.addOFMessageListener(OFType.ERROR, this);
    	 floodlightProvider.addOFMessageListener(OFType.ECHO_REQUEST, this);
    	 floodlightProvider.addOFMessageListener(OFType.ECHO_REPLY, this);
    	 floodlightProvider.addOFMessageListener(OFType.VENDOR, this);
    	 floodlightProvider.addOFMessageListener(OFType.FEATURES_REQUEST, this);
    	 floodlightProvider.addOFMessageListener(OFType.FEATURES_REPLY, this);
    	 floodlightProvider.addOFMessageListener(OFType.GET_CONFIG_REPLY, this);
    	 floodlightProvider.addOFMessageListener(OFType.SET_CONFIG, this);
    	 floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
    	 floodlightProvider.addOFMessageListener(OFType.FLOW_REMOVED, this);
    	 floodlightProvider.addOFMessageListener(OFType.PORT_STATUS, this);
    	 floodlightProvider.addOFMessageListener(OFType.PACKET_OUT, this);
    	 floodlightProvider.addOFMessageListener(OFType.FLOW_MOD, this);
    	 floodlightProvider.addOFMessageListener(OFType.PORT_MOD, this);
    	 floodlightProvider.addOFMessageListener(OFType.STATS_REQUEST, this);
    	 floodlightProvider.addOFMessageListener(OFType.STATS_REPLY, this);
    	 floodlightProvider.addOFMessageListener(OFType.BARRIER_REQUEST, this);
    	 floodlightProvider.addOFMessageListener(OFType.BARRIER_REPLY, this);

        IStorageSourceListener listen = new IStorageSourceListener() {
			@Override
			public void rowsModified(String tableName, Set<Object> rowKeys) {
				if(tableName.equalsIgnoreCase("controller_switch")){
					for(Object row : rowKeys)
						System.out.println("Controller switch attach: " + row.toString());
				}else if(tableName.equalsIgnoreCase("controller_port")){
					for(Object row : rowKeys){
						int lastIndexOfDoublePoints = row.toString().lastIndexOf(':');
						System.out.println("Controller port attach: " + row.toString().substring(0,lastIndexOfDoublePoints) + row.toString().substring(lastIndexOfDoublePoints+1));
					}
				}else if(tableName.equalsIgnoreCase("controller_host")){
					for(Object row : rowKeys){
						System.out.println("Controller host attach: " + row.toString());
					}
				}else if(tableName.equalsIgnoreCase("controller_link")){
						for(Object row : rowKeys){
							String parts[] = row.toString().split("-");
						System.out.println("Controller link attach: " + parts[0] + parts[1] + parts[2] + parts[3]);
					}
				}else if(tableName.equalsIgnoreCase("controller_hostattachmentpoint")){
					for(Object row : rowKeys){
						String parts[] = row.toString().split("-");
						System.out.println("Controller hostattachmentpoint attach: " + parts[0] + parts[1] + parts[2]);
					}
				}
			}
			
			@Override
			public void rowsDeleted(String tableName, Set<Object> rowKeys) {
				if(tableName.equalsIgnoreCase("controller_switch")){
					for(Object row : rowKeys)
						System.out.println("Controller switch remove: " + row.toString());
				}else if(tableName.equalsIgnoreCase("controller_port")){
					for(Object row : rowKeys){
						int lastIndexOfDoublePoints = row.toString().lastIndexOf(':');
						System.out.println("Controller port remove: " + row.toString().substring(0,lastIndexOfDoublePoints) + row.toString().substring(lastIndexOfDoublePoints+1));
					}
				}else if(tableName.equalsIgnoreCase("controller_host")){
					for(Object row : rowKeys){
						System.out.println("Controller host remove: " + row.toString());
					}
				}else
					if(tableName.equalsIgnoreCase("controller_link")){
						for(Object row : rowKeys){
							String parts[] = row.toString().split("-");
							System.out.println("Controller link remove: " + parts[0] + parts[1] + parts[2] + parts[3]);
					}
				}else if(tableName.equalsIgnoreCase("controller_hostattachmentpoint")){
					for(Object row : rowKeys){
						String parts[] = row.toString().split("-");
						System.out.println("Controller hostattachmentpoint remove: " + parts[0] + parts[1] + parts[2]);
					}
				}
			}
		};
        
		
        storageSource.addListener("controller_switch", listen);
		storageSource.addListener("controller_port", listen);
		storageSource.addListener("controller_host", listen);
		storageSource.addListener("controller_link", listen);
		storageSource.addListener("controller_hostattachmentpoint", listen);
		
    }

    @Override
    public String getName() {
        return "dts-dealer";
    }

    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        return false;
    }

    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        return false;
    }

    private void writePacketOutForPacketIn(IOFSwitch sw, byte[] packetData, short egressPort) {
    	OFPacketOut packetOutMessage = (OFPacketOut) floodlightProvider.getOFMessageFactory().getMessage(OFType.PACKET_OUT);
    	short packetOutLength = (short)OFPacketOut.MINIMUM_LENGTH; // starting length

    	//Set buffer_id, actions_len
    	packetOutMessage.setBufferId(OFPacketOut.BUFFER_ID_NONE);
    	packetOutMessage.setActionsLength((short)OFActionOutput.MINIMUM_LENGTH);
    	packetOutLength += OFActionOutput.MINIMUM_LENGTH;

    	// Set actions
    	List<OFAction> actions = new ArrayList<OFAction>(1);      
    	actions.add(new OFActionOutput(egressPort, (short) 0));
    	packetOutMessage.setActions(actions);

    	// Set packet data
   		packetOutMessage.setPacketData(packetData); 
   		packetOutLength += (short)packetData.length;

    	// Set the total length
    	packetOutMessage.setLength(packetOutLength);              

    	// Write it out
    	try {
    		sw.write(packetOutMessage, null);
    	} catch (IOException e) {
    		logger.error("Failed to write {} to switch {}: {}", new Object[]{ packetOutMessage, sw, e });
    	}
}
    
    
    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        OFPacketIn pi = (OFPacketIn) msg;
        
        System.out.println("Tipo da msg no receiver: "  + msg.getType());
        
        return Command.CONTINUE;
    }



	@Override
    public int getId() {
		//TODO
		return 0;
    }

	public void setStorageSource(IStorageSource storageSource) {
		this.storageSource = storageSource;
	}

	public void setFlowEntryPusher(IStaticFlowEntryPusher staticFlowEntryPusher) {
		this.staticFlowEntryPusher = staticFlowEntryPusher;
	}

}