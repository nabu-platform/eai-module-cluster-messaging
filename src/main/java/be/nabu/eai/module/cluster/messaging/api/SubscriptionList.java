package be.nabu.eai.module.cluster.messaging.api;

import java.util.List;

/**
 * We still don't quite care which server is interested in a particular subscription because we are broadcasting to all every time
 * However, if a server were to unsubscribe to a particular subscription but another server is still interested, we still need to broadcast it
 * To that end, we _do_ want to know the server so we can keep a tally of the amount of servers interested
 */
public interface SubscriptionList {
	public String getServer();
	/**
	 * This must always contain all subscriptions the server is interested in
	 * Any subscription not mentioned here is assumed to be unsubscribed
	 */
	public List<? extends Subscription> getSubscriptions();
}
