package be.nabu.eai.module.cluster.messaging;

import java.util.List;

import be.nabu.eai.module.cluster.messaging.api.Subscription;

public class MultiSubscription {
	private Subscription subscription;
	private List<String> servers;
	public Subscription getSubscription() {
		return subscription;
	}
	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}
	public List<String> getServers() {
		return servers;
	}
	public void setServers(List<String> servers) {
		this.servers = servers;
	}
	@Override
	public boolean equals(Object object) {
		return object instanceof MultiSubscription && subscription.getId().equals(((MultiSubscription) object).getSubscription().getId());
	}
	@Override
	public int hashCode() {
		return subscription.getId().hashCode();
	}
}
