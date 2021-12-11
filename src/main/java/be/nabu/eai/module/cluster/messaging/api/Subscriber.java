package be.nabu.eai.module.cluster.messaging.api;

import be.nabu.libs.types.api.ComplexContent;

public interface Subscriber {
	public String getServiceId();
	public ComplexContent getInput();
	public String getId();
}
