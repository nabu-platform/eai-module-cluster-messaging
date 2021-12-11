package be.nabu.eai.module.cluster.messaging;

import be.nabu.eai.module.cluster.messaging.api.Subscriber;
import be.nabu.eai.module.cluster.messaging.api.Subscription;
import be.nabu.eai.module.cluster.messaging.api.SubscriptionSubscriber;

public class SubscriptionSubscriberImpl implements SubscriptionSubscriber {

	private String id;
	private Subscription subscription;
	private Subscriber subscriber;
	
	@Override
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	@Override
	public Subscription getSubscription() {
		return subscription;
	}
	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}
	
	@Override
	public Subscriber getSubscriber() {
		return subscriber;
	}
	public void setSubscriber(Subscriber subscriber) {
		this.subscriber = subscriber;
	}

}
