package be.nabu.eai.module.cluster.messaging;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import be.nabu.eai.module.cluster.messaging.api.SubscriptionList;

public class SubscriptionListImpl implements SubscriptionList, Serializable {
	private static final long serialVersionUID = 1L;
	
	private Date created;
	private List<SubscriptionImpl> subscriptions;
	private String server;
	
	@Override
	public List<SubscriptionImpl> getSubscriptions() {
		return subscriptions;
	}
	public void setSubscriptions(List<SubscriptionImpl> subscriptions) {
		this.subscriptions = subscriptions;
	}

	@Override
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}
	
}
