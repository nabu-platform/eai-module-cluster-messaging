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
import java.util.ArrayList;
import java.util.List;

public class BroadcastMessage implements Serializable {
	private static final long serialVersionUID = 1L;
	
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
