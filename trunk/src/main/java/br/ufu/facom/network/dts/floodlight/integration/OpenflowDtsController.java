package br.ufu.facom.network.dts.floodlight.integration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.OFMessageFilterManager;
import net.floodlightcontroller.core.internal.Controller;
import net.floodlightcontroller.core.internal.OpenflowPipelineFactory;
import net.floodlightcontroller.counter.CounterStore;
import net.floodlightcontroller.devicemanager.internal.DeviceManagerImpl;
import net.floodlightcontroller.learningswitch.LearningSwitch;
import net.floodlightcontroller.perfmon.PktinProcessingTime;
import net.floodlightcontroller.staticflowentry.StaticFlowEntryPusher;
import net.floodlightcontroller.storage.IStorageSourceListener;
import net.floodlightcontroller.storage.StorageException;
import net.floodlightcontroller.storage.memory.MemoryStorageSource;
import net.floodlightcontroller.topology.ITopologyAware;
import net.floodlightcontroller.topology.internal.TopologyImpl;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public class OpenflowDtsController extends Controller{
	protected DtsModule dtsDealer;
	
	protected StaticFlowEntryPusher staticFlowEntryPusher;
	
	@Override
	protected void init() {
		storageSource = new MemoryStorageSource();
		deviceManager = new DeviceManagerImpl();
		topology = new TopologyImpl();
		pktinProcTime = new PktinProcessingTime();
		counterStore = new CounterStore();
		
		this.setStorageSource(storageSource);      
		
		deviceManager.setFloodlightProvider(this);
        deviceManager.setStorageSource(storageSource);
        deviceManager.setTopology(topology);
		
		topology.setFloodlightProvider(this);
        topology.setStorageSource(storageSource);
        HashSet<ITopologyAware> topologyAware = new HashSet<ITopologyAware>();
        topologyAware.add(deviceManager);
        topology.setTopologyAware(topologyAware);

        //messageFilterManager = new OFMessageFilterManager();
        //messageFilterManager.init(this);

        staticFlowEntryPusher = new StaticFlowEntryPusher();
        staticFlowEntryPusher.setFloodlightProvider(this);
        
		dtsDealer = new DtsModule();
		dtsDealer.setFloodlightProvider(this);
		dtsDealer.setStorageSource(storageSource);
		dtsDealer.setFlowEntryPusher(staticFlowEntryPusher);
	}
	
	@Override
	protected void startupComponents() {
        try {
            log.debug("Doing controller internal setup");
            this.startUp();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
		log.info("Starting the DtsDealer...");
		dtsDealer.startUp();
		
		log.debug("Starting topology service");
        topology.startUp();
        
        log.debug("Starting deviceManager service");
        deviceManager.startUp();
		
        //log.debug("Starting messageFilter service");
        //messageFilterManager.startUp();
        
        log.debug("Starting counterStore service");
        counterStore.startUp();
        
        log.debug("Starting staticFlowEntryPusher service");
        staticFlowEntryPusher.startUp();
	}
	protected void run() {
        try {            
            final ServerBootstrap bootstrap = new ServerBootstrap(
                    new NioServerSocketChannelFactory(
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool()));

            bootstrap.setOption("reuseAddr", true);
            bootstrap.setOption("child.keepAlive", true);
            bootstrap.setOption("child.tcpNoDelay", true);

            ChannelPipelineFactory pfact = 
                    new OpenflowPipelineFactory(this, null);
            bootstrap.setPipelineFactory(pfact);
            InetSocketAddress sa = new InetSocketAddress(OPENFLOW_PORT);
            final ChannelGroup cg = new DefaultChannelGroup();
            cg.add(bootstrap.bind(sa));
            
            log.info("Listening for switch connections on {}", sa);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // main loop
        while (true) {
            try {
                Update update = updates.take();
                log.debug("Dispatching switch update {} {}",
                          update.sw, update.added);
                if (switchListeners != null) {
                    for (IOFSwitchListener listener : switchListeners) {
                        if (update.added)
                            listener.addedSwitch(update.sw);
                        else
                            listener.removedSwitch(update.sw);
                    }
                }
            } catch (InterruptedException e) {
                return;
            } catch (StorageException e) {
                log.error("Storage exception in controller " + 
                          "updates loop; terminating process",
                          e);
                return;
            } catch (Exception e) {
                log.error("Exception in controller updates loop", e);
            }
        }
    }
	
	/** 
     * Main function entry point; override init() for adding modules
     * @param args
     */
    
    public static void main(String args[]) throws Exception {
        OpenflowDtsController controller = new OpenflowDtsController();
        controller.init();
        controller.startupComponents();
        controller.run();
    }
}
