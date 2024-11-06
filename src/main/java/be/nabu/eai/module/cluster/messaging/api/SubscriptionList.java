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

package be.nabu.eai.module.cluster.messaging.api;

import java.util.List;

/**
 * We still don't quite care which server is interested in a particular subscription because we are broadcasting to all every time
 * However, if a server were to unsubscribe to a particular subscription but another server is still interested, we still need to broadcast it
 * To that end, we _do_ want to know the server so we can keep a tally of the amount of servers interested
 */
public interface SubscriptionList {
	public String getServer();
	/**
	 * This must always contain all subscriptions the server is interested in
	 * Any subscription not mentioned here is assumed to be unsubscribed
	 */
	public List<? extends Subscription> getSubscriptions();
}
