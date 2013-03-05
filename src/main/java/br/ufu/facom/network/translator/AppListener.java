package br.ufu.facom.network.translator;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFSwitch;

import org.openflow.protocol.OFMessage;



public interface AppListener {

	/**
	 * Receives an Event from the OPENFLOW and sends it to the SLEE.
	 * 
	 * @param sw
	 * @param msg
	 * @param cntx
	 */
	public void sendEvent(IOFSwitch ioFSwitch, OFMessage ofMessage, FloodlightContext floodlightContext);
	
	
}

