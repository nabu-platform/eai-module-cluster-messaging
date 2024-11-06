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

package nabu.misc.broadcast;

import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;

import be.nabu.eai.module.cluster.messaging.MessageListener;
import be.nabu.eai.module.cluster.messaging.api.SubscriptionSubscriber;

@WebService
public class Services {
	
	@WebResult(name = "subscriptionId")
	public String subscribe(@WebParam(name = "typeId") String typeId,
			@WebParam(name = "query") String query,
			@WebParam(name = "serviceId") String serviceId,
			@WebParam(name = "input") Object input) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			return instance.subscribe(typeId, query, null, serviceId, input);
		}
		return null;
	}
	
	public void unsubscribe(@WebParam(name = "subscriptionId") String subscriptionId) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			instance.unsubscribe(subscriptionId);
		}
	}
	
	public void fire(@WebParam(name = "data") Object data) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			instance.publish(data);
		}
	}
	
	@WebResult(name = "result")
	public SubscriptionSubscriber resolve(@WebParam(name = "subscriptionId") String subscriptionId) {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			return instance.resolve(subscriptionId);
		}
		return null;
	}

	@WebResult(name = "results")
	public List<SubscriptionSubscriber> list() {
		MessageListener instance = MessageListener.getInstance();
		if (instance != null) {
			return instance.listAll();
		}
		return null;
	}
}
