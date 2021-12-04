package be.nabu.eai.module.cluster.messaging;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import be.nabu.eai.module.cluster.messaging.api.Subscription;

public class SubscriptionImpl implements Subscription, Serializable {
	private static final long serialVersionUID = 1L;
	
	private String id, topicId, typeId, query;
	private List<String> servers;
	
	@Override
	public String getId() {
		if (id == null) {
			try {
				// we are assuming identical queries are identical and not differing on for example whitespace
				String idContent = getTopicId() + "::" + getTypeId() + "::" + (query == null ? "$all" : query);
				return digest(idContent);
			}
			catch (Exception e) {
				// should not occur
				throw new RuntimeException(e);
			}
		}
		return id;
	}

	public static String digest(String idContent) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest digest = MessageDigest.getInstance("SHA-1");
		digest.update((byte[]) idContent.getBytes("UTF-8"));
		byte [] hash = digest.digest();
		StringBuilder string = new StringBuilder();
		for (int i = 0; i < hash.length; ++i) {
			string.append(Integer.toHexString((hash[i] & 0xFF) | 0x100).substring(1,3));
		}
		return string.toString();
	}

	@Override
	public String getTopicId() {
		return topicId == null ? "$default" : topicId;
	}

	@Override
	public String getTypeId() {
		return typeId;
	}

	@Override
	public String getQuery() {
		return query;
	}

	@Override
	public boolean equals(Object object) {
		return object instanceof Subscription && getId().equals(((Subscription) object).getId());
	}
	
	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	public List<String> getServers() {
		return servers;
	}

	public void setServers(List<String> servers) {
		this.servers = servers;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}

	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
}
