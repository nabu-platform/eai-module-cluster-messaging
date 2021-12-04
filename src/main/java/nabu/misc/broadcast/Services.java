package nabu.misc.broadcast;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;

import be.nabu.eai.module.cluster.messaging.MessageListener;

@WebService
public class Services {
	
	@WebResult(name = "subscriptionId")
	public String subscribe(@WebParam(name = "typeId") String typeId,
			@WebParam(name = "query") String query,
			@WebParam(name = "serviceId") String serviceId,
			@WebParam(name = "input") Object input) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			return instance.subscribe(typeId, query, null, serviceId, input);
		}
		return null;
	}
	
	public void unsubscribe(@WebParam(name = "subscriptionId") String subscriptionId) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			instance.unsubscribe(subscriptionId);
		}
	}
	
	public void fire(@WebParam(name = "data") Object data) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			instance.publish(data);
		}
	}
}
