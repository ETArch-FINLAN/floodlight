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

import br.ufu.facom.network.dlontology.msg.DLParser;
import br.ufu.facom.network.dlontology.msg.Message;
import br.ufu.facom.network.dlontology.msg.SimpleParser;
import br.ufu.facom.network.dts.DTSAgent;
import br.ufu.facom.network.dts.bean.core.Entity;
import br.ufu.facom.network.dts.bean.core.Workspace;
import br.ufu.facom.network.dts.topology.Port;
import br.ufu.facom.network.dts.topology.Switch;
import br.ufu.facom.network.dts.util.Constants;
import br.ufu.facom.network.dts.util.WorkspaceConfigPusher;
/**
 * Keep track of source MAC addresses and log when we see new ones.
 */
public class DtsModule implements IOFMessageListener {
	/**
	 * The DTS Agent, and DL-Ontology Parser
	 */
	DTSAgent dts = new DTSAgent(true);
	DLParser parser = new SimpleParser();
	
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
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);

        IStorageSourceListener listen = new IStorageSourceListener() {
			@Override
			public void rowsModified(String tableName, Set<Object> rowKeys) {
				if(tableName.equalsIgnoreCase("controller_switch")){
					for(Object row : rowKeys)
						dts.registerSwitch(row.toString());
				}else if(tableName.equalsIgnoreCase("controller_port")){
					for(Object row : rowKeys){
						int lastIndexOfDoublePoints = row.toString().lastIndexOf(':');
						dts.addSwitchPort(row.toString().substring(0,lastIndexOfDoublePoints), row.toString().substring(lastIndexOfDoublePoints+1));
					}
				}else if(tableName.equalsIgnoreCase("controller_host")){
					for(Object row : rowKeys){
						dts.registerHost(row.toString());
					}
				}else if(tableName.equalsIgnoreCase("controller_link")){
						for(Object row : rowKeys){
							String parts[] = row.toString().split("-");
							dts.addLink(parts[0],parts[1],parts[2],parts[3]);
					}
				}else if(tableName.equalsIgnoreCase("controller_hostattachmentpoint")){
					for(Object row : rowKeys){
						String parts[] = row.toString().split("-");
						dts.attachHost(parts[0],parts[1],parts[2]);
					}
				}
			}
			
			@Override
			public void rowsDeleted(String tableName, Set<Object> rowKeys) {
				if(tableName.equalsIgnoreCase("controller_switch")){
					for(Object row : rowKeys)
						dts.unregisterSwitch(row.toString());
				}else if(tableName.equalsIgnoreCase("controller_port")){
					for(Object row : rowKeys){
						int lastIndexOfDoublePoints = row.toString().lastIndexOf(':');
						dts.deleteSwitchPort(row.toString().substring(0,lastIndexOfDoublePoints), row.toString().substring(lastIndexOfDoublePoints+1));
					}
				}else if(tableName.equalsIgnoreCase("controller_host")){
					for(Object row : rowKeys){
						dts.unregisterHost(row.toString());
					}
				}else
					if(tableName.equalsIgnoreCase("controller_link")){
						for(Object row : rowKeys){
							String parts[] = row.toString().split("-");
							dts.deleteLink(parts[0],parts[1],parts[2],parts[3]);
					}
				}else if(tableName.equalsIgnoreCase("controller_hostattachmentpoint")){
					for(Object row : rowKeys){
						String parts[] = row.toString().split("-");
						dts.detachHost(parts[0],parts[1],parts[2]);
					}
				}
			}
		};
        
		
        storageSource.addListener("controller_switch", listen);
		storageSource.addListener("controller_port", listen);
		storageSource.addListener("controller_host", listen);
		storageSource.addListener("controller_link", listen);
		storageSource.addListener("controller_hostattachmentpoint", listen);
		
		dts.registerWorkspaceConfigPusher(new WorkspaceConfigPusher() {
			
			@Override
			public void removePortFromWorkspace(Workspace ws, Port port) {
				//TODO
				logger.error("Not implemented yet!");
			}
			
			@Override
			public void recreateWorkspace(Workspace ws, java.util.Set<Port> path) {
				logger.debug("Publishing a workspace...");
				
				java.util.HashMap<Switch,List<Port>> mapWs = new java.util.HashMap<Switch,List<Port>>();
				java.util.HashSet<Long> swSet = new java.util.HashSet<Long>();
				
				for(Port port : path){
					if(!mapWs.containsKey(port.getParent())){
						mapWs.put(port.getParent(), new ArrayList<Port>());
					}
					
					swSet.add(getDataPathBySwitch(port.getParent().getTitle()));
					mapWs.get(port.getParent()).add(port);
				}
				
				OFMatch match = new OFMatch();
				match.fromString("dl_vlan="+ws.getId());

				for(long datapath : staticFlowEntryPusher.getEntries().keySet()){
					if(!swSet.contains(datapath)){
						if(staticFlowEntryPusher.getEntries().get(datapath).containsKey(ws.getTitle())){
							for(IOFSwitch sw : staticFlowEntryPusher.getActiveSwitches()){
								if(sw.getId() == datapath){
									staticFlowEntryPusher.removeEntry(sw, ws.getTitle());
									break;
								}
							}
						}
					}
				}
				
				for(Switch sw : mapWs.keySet()){
					short packetOutLength = (short)OFFlowMod.MINIMUM_LENGTH; // starting length
					
					List<OFAction> listActions = new ArrayList<OFAction>();
					for(Port port : mapWs.get(sw)){
						listActions.add(new OFActionOutput((short)port.getIndex(),(short) 0));
						packetOutLength += OFActionOutput.MINIMUM_LENGTH;
					}
					
					OFFlowMod flowMod = (OFFlowMod) floodlightProvider.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
					flowMod.setActions(listActions);
					flowMod.setMatch(match);
					flowMod.setLength(packetOutLength);
					
					
					long dataPath = getDataPathBySwitch(sw.getTitle());
					
					if(dataPath >= 0){
						try {
							IOFSwitch iofSw = floodlightProvider.getSwitches().get(dataPath);
							flowMod.setBufferId(iofSw.getNextTransactionId());
							iofSw.write(flowMod, null);
							
							//To static flow pusher
							flowMod.setBufferId(iofSw.getNextTransactionId());
						} catch (IOException e) {
							logger.error("It wasn't possible to configure the switch "+sw.getTitle());
						}
						
						staticFlowEntryPusher.addEntry(dataPath,ws.getTitle(), true, flowMod);
					}else{
						logger.error("Unknown datapath for switch "+sw.getTitle());
					}
					
				}
			}
			
			private long getDataPathBySwitch(String title) {
				for(long dataPath : floodlightProvider.getSwitches().keySet()){
					if(floodlightProvider.getSwitches().get(dataPath).getStringId().equalsIgnoreCase(title))
						return dataPath;
				}
				return -1;
			}

			@Override
			public void addPortToWorkspace(Workspace ws, Port port) {
				logger.error("Not implemented yet!");
			}

			@Override
			public void deleteWorkspace(Workspace workspace) {
				for(long datapath : staticFlowEntryPusher.getEntries().keySet()){
					if(staticFlowEntryPusher.getEntries().get(datapath).containsKey(workspace.getTitle())){
						for(IOFSwitch sw : staticFlowEntryPusher.getActiveSwitches()){
							if(sw.getId() == datapath){
								staticFlowEntryPusher.removeEntry(sw, workspace.getTitle());
								break;
							}
						}
					}
				}
			}
		});
		
		/**
		 * Just a test
		 */
//		new Thread(){
//			public void run() {
//				try {
//					Thread.sleep(6000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				
//				Element e1 = new Element();
//				e1.setTitle("00:00:00:00:00:00:00:04");
//				e1.addPort("1",1);
//				
//				Workspace ws = new Workspace();
//				ws.setTitle("ws");
//				ws.addAllPortsToPath(e1.getPorts());
//				ws.setId(2);
//				
//				dts.getWorkspaceConfigPusher().recreateWorkspace(ws);
//			};
//		}.start();
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
        
        if(parser.validStartMessage(pi.getPacketData())){
        	Message inMessage = parser.parseMessage(pi.getPacketData());
        	if(inMessage.getDestination().equals(Constants.dtsWorkspace)){
        		Message outMessage = dts.onMessage(inMessage,sw.getStringId(),(""+pi.getInPort()));
        		if(outMessage != null){
        			writePacketOutForPacketIn(sw, parser.parse(outMessage), pi.getInPort());
        		}
        	}
        }

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