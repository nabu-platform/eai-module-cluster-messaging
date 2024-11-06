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

import java.util.List;

import be.nabu.eai.module.cluster.messaging.api.Subscription;

public class MultiSubscription {
	private Subscription subscription;
	private List<String> servers;
	public Subscription getSubscription() {
		return subscription;
	}
	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}
	public List<String> getServers() {
		return servers;
	}
	public void setServers(List<String> servers) {
		this.servers = servers;
	}
	@Override
	public boolean equals(Object object) {
		return object instanceof MultiSubscription && subscription.getId().equals(((MultiSubscription) object).getSubscription().getId());
	}
	@Override
	public int hashCode() {
		return subscription.getId().hashCode();
	}
}
