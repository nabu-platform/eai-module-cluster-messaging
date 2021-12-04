package be.nabu.eai.module.cluster.messaging.api;

/**
 * We don't particularly care which server initiated the subscription.
 * We are using topics, not queues so we are not sending it to specific servers but rather all servers.
 * Otherwise, we would need point-to-point connections to every server and possibly send on multiple queues if multiple are interested.
 */
public interface Subscription {
	// the id of the subscription
	// a subscription should be uniquely identified by the composite of topic, type & query
	// the id could for example be a hash of that
	public String getId();
	// an optional topic, otherwise you listen to the default topic
	public String getTopicId();
	// a mandatory type you are listening to
	public String getTypeId();
	// an optional query
	public String getQuery();
}
