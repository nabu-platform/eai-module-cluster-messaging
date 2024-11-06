/*
* Copyright (C) 2021 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.module.cluster.messaging;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.module.cluster.messaging.api.Subscriber;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.binding.json.JSONBinding;

public class SubscriberImpl implements Subscriber {
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private String id;
	private String serviceId;
	private ComplexContent input;
	
	@Override
	public String getServiceId() {
		return serviceId;
	}
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	
	@Override
	public ComplexContent getInput() {
		return input;
	}
	public void setInput(ComplexContent input) {
		this.input = input;
	}
	
	@Override
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
		}
		return active;
	}
}
