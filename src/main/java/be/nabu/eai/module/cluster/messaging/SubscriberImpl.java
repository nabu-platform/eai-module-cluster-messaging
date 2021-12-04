package be.nabu.eai.module.cluster.messaging;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.binding.json.JSONBinding;

public class SubscriberImpl {
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private String id;
	private String serviceId;
	private ComplexContent input;
	
	public String getServiceId() {
		return serviceId;
	}
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	public ComplexContent getInput() {
		return input;
	}
	public void setInput(ComplexContent input) {
		this.input = input;
	}
	public String getId() {
		if (id == null) {
			try {
				ByteArrayOutputStream key = new ByteArrayOutputStream();
				key.write(serviceId.getBytes(Charset.forName("UTF-8")));
				if (input != null) {
					key.write("::".getBytes(Charset.forName("UTF-8")));
					JSONBinding binding = new JSONBinding(input.getType(), Charset.forName("UTF-8"));
					binding.marshal(key, input);
				}
				id = SubscriptionImpl.digest(new String(key.toByteArray(), Charset.forName("UTF-8")));
			}
			catch (Exception e) {
				throw new IllegalArgumentException("Can not serialize the subscriber key", e);
			}
		}
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	// returns whether or not active still
	public boolean fire(Repository repository, String subscriptionId, String typeId, ComplexContent content) {
		boolean active = false;
		Artifact resolve = repository.resolve(serviceId);
		if (resolve instanceof DefinedService) {
			DefinedService service = ((DefinedService) resolve);
			// always create a new input, we don't want to overwrite the input we have by reference, this might cause concurrency issues
			ComplexContent newInstance = service.getServiceInterface().getInputDefinition().newInstance();
			if (input != null) {
				for (Element<?> child : TypeUtils.getAllChildren(input.getType())) {
					newInstance.set(child.getName(), input.get(child.getName()));
				}
			}
			newInstance.set("subscriptionId", subscriptionId);
			newInstance.set("typeId", typeId);
			newInstance.set("data", content);
			
			ServiceRuntime runtime = new ServiceRuntime(service, repository.newExecutionContext(SystemPrincipal.ROOT));
			try {
				runtime.registerInThread(false);
				ComplexContent run = runtime.run(newInstance);
				if (run != null) {
					Object object = run.get("active");
					if (object instanceof Boolean && (Boolean) object) {
						active = true;
					}
				}
			}
			catch (Exception e) {
				logger.error("Exception thrown by message subscriber", e);
			}
			finally {
				runtime.unregisterInThread();
			}
		}
		return active;
	}
}
