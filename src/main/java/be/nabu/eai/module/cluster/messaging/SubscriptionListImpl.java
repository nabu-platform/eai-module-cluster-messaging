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

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import be.nabu.eai.module.cluster.messaging.api.SubscriptionList;

public class SubscriptionListImpl implements SubscriptionList, Serializable {
	private static final long serialVersionUID = 1L;
	
	private Date created;
	private List<SubscriptionImpl> subscriptions;
	private String server;
	
	@Override
	public List<SubscriptionImpl> getSubscriptions() {
		return subscriptions;
	}
	public void setSubscriptions(List<SubscriptionImpl> subscriptions) {
		this.subscriptions = subscriptions;
	}

	@Override
	public String getServer() {
		return server;
	}
	public void setServer(String server) {
		this.server = server;
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}
	
}
