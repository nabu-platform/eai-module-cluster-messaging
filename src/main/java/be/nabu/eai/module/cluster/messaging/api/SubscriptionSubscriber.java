package be.nabu.eai.module.cluster.messaging.api;

public interface SubscriptionSubscriber {
	public String getId();
	public Subscription getSubscription();
	public Subscriber getSubscriber();
}
