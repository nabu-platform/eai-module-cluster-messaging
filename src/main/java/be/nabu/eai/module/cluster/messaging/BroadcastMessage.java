package be.nabu.eai.module.cluster.messaging;

import java.util.ArrayList;
import java.util.List;

public class BroadcastMessage {
	// stringified content of the actual message
	private String content;
	// the type of the message
	private String typeId;
	// the subscriptions that are triggered by this message
	private List<String> subscriptionIds;
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getTypeId() {
		return typeId;
	}
	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}
	public List<String> getSubscriptionIds() {
		if (subscriptionIds == null) {
			subscriptionIds = new ArrayList<String>();
		}
		return subscriptionIds;
	}
	public void setSubscriptionIds(List<String> subscriptionIds) {
		this.subscriptionIds = subscriptionIds;
	}
}
