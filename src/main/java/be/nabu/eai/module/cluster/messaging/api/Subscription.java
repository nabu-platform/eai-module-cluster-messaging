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

/**
 * We don't particularly care which server initiated the subscription.
 * We are using topics, not queues so we are not sending it to specific servers but rather all servers.
 * Otherwise, we would need point-to-point connections to every server and possibly send on multiple queues if multiple are interested.
 */
public interface Subscription {
	// the id of the subscription
	// a subscription should be uniquely identified by the composite of topic, type & query
	// the id could for example be a hash of that
	public String getId();
	// an optional topic, otherwise you listen to the default topic
	public String getTopicId();
	// a mandatory type you are listening to
	public String getTypeId();
	// an optional query
	public String getQuery();
}
