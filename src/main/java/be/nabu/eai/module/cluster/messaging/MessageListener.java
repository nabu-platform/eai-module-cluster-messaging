package be.nabu.eai.module.cluster.messaging;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.server.Server;
import be.nabu.eai.server.api.ServerListener;
import be.nabu.libs.cluster.api.ClusterInstance;
import be.nabu.libs.cluster.api.ClusterMessageListener;
import be.nabu.libs.cluster.api.ClusterTopic;
import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.evaluator.PathAnalyzer;
import be.nabu.libs.evaluator.QueryParser;
import be.nabu.libs.evaluator.impl.VariableOperation;
import be.nabu.libs.evaluator.types.api.TypeOperation;
import be.nabu.libs.evaluator.types.operations.TypesOperationProvider;
import be.nabu.libs.http.api.server.HTTPServer;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.binding.json.JSONBinding;

// startup service that makes sure everything is running

// TODO: periodic synchronization of subscriptions + direct synchronization if new subscription is added (not new input on a service! but really new subscription)
/**
 * Currently there is only one topic ($default)
 * In the future, new topics will be allowed, but this will likely be with a dedicated artifact
 * At design time you are targetting a topic and it must exist, hence it should be part of the repository.
 * 
 * Either way, the lifecycle of a topic (when it gets destroyed) is not controlled by the subscriptions, but rather by the artifacts themselves
 * The default topics are never destroyed.
 */
public class MessageListener implements ServerListener {

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static MessageListener instance;
	// these are the subscriptions we check when new data arrives
	// we are not using volatile, slight delays in synchronization are OK
	private Map<String, List<SubscriptionImpl>> subscriptionsByType = new HashMap<String, List<SubscriptionImpl>>();
	
	// these are the subscriptions we check when a server updates its subscription list
	private Map<String, SubscriptionListImpl> subscriptionsByServer = new HashMap<String, SubscriptionListImpl>();
	
	public static MessageListener getInstance() {
		return instance;
	}
	
	@Override
	public void listen(Server server, HTTPServer httpServer) {
		instance = this;
		cluster = server.getCluster();
		
		// we listen to the subscription topic to keep subscriptions up to date
		ClusterTopic<SubscriptionListImpl> subscriptionTopic = cluster.topic("messaging.$subscriptions");
		subscriptionTopic.subscribe(new ClusterMessageListener<SubscriptionListImpl>() {
			@Override
			public void onMessage(SubscriptionListImpl message) {
				try {
					processSubscriptionList(message);
				}
				catch (Exception e) {
					logger.error("Could not process heartbeat", e);
				}
			}
		});
		
		ClusterTopic<BroadcastMessage> defaultTopic = cluster.topic("messaging.$default");
		defaultTopic.subscribe(new ClusterMessageListener<BroadcastMessage>() {
			@Override
			public void onMessage(BroadcastMessage message) {
				try {
					processMessage(message);
				}
				catch (Exception e) {
					logger.error("Could not process heartbeat", e);
				}
			}
		});
	}
	
	private void processMessage(BroadcastMessage message) {
		// TODO
	}
	
	synchronized private void processSubscriptionList(SubscriptionListImpl subscriptions) {
		// we re-timestamp to avoid server time synchronization issues
		// it only serves to detect timeouts
		subscriptions.setCreated(new Date());
		
		SubscriptionListImpl previous = subscriptionsByServer.get(subscriptions.getServer());
		List<SubscriptionImpl> subscriptionsToAdd = new ArrayList<SubscriptionImpl>();
		List<SubscriptionImpl> subscriptionsToRemove = new ArrayList<SubscriptionImpl>();
		// at first, we consider every subscription new
		if (subscriptions.getSubscriptions() != null && !subscriptions.getSubscriptions().isEmpty()) {
			subscriptionsToAdd.addAll(subscriptions.getSubscriptions());
		}
		// if we have a previous subscription, we must unsubscribe to any that are no longer in the new list
		// and we don't have to resubscribe to what we are already interested in
		if (previous != null) {
			// the previous must have subscriptions or it would not have gotten into the map
			List<SubscriptionImpl> previousSubscriptions = previous.getSubscriptions();
			subscriptionsToRemove.addAll(previousSubscriptions);
			// don't re-subscribe
			subscriptionsToAdd.removeAll(previousSubscriptions);
			// if we have new subscriptions, they must be maintained
			if (subscriptions.getSubscriptions() != null && !subscriptions.getSubscriptions().isEmpty()) {
				subscriptionsToRemove.removeAll(subscriptions.getSubscriptions());
			}
		}
		
		// we only start more logic if you actually want to change something
		if (!subscriptionsToAdd.isEmpty() || !subscriptionsToRemove.isEmpty()) {
			// we take a "local" copy of the map to manipulate to prevent concurrent access with publishing without taking a lock
			// this method is synchronized partially to prevent multiple instances from doing separate modifications
			Map<String, List<SubscriptionImpl>> subscriptionsByType = new HashMap<String, List<SubscriptionImpl>>(this.subscriptionsByType);
			for (SubscriptionImpl subscriptionToAdd : subscriptionsToAdd) {
				List<SubscriptionImpl> typeSubscriptions = subscriptionsByType.get(subscriptionToAdd.getTypeId());
				if (typeSubscriptions == null) {
					typeSubscriptions = new ArrayList<SubscriptionImpl>();
					subscriptionsByType.put(subscriptionToAdd.getTypeId(), typeSubscriptions);
				}
				int indexOf = typeSubscriptions.indexOf(subscriptionToAdd);
				// if the subscription already exist, just make sure the server is in there
				if (indexOf >= 0) {
					if (!typeSubscriptions.get(indexOf).getServers().contains(subscriptions.getServer())) {
						typeSubscriptions.get(indexOf).getServers().add(subscriptions.getServer());
					}
				}
				// otherwise, we add the subscription with the current server
				else {
					ArrayList<String> servers = new ArrayList<String>();
					servers.add(subscriptions.getServer());
					subscriptionToAdd.setServers(servers);
					typeSubscriptions.add(subscriptionToAdd);
				}
			}
			for (SubscriptionImpl subscriptionToRemove : subscriptionsToRemove) {
				List<SubscriptionImpl> typeSubscriptions = subscriptionsByType.get(subscriptionToRemove.getTypeId());
				// it should not be null at this point, but we'll roll with it
				if (typeSubscriptions != null) {
					int indexOf = typeSubscriptions.indexOf(subscriptionToRemove);
					// again, it should be there
					if (indexOf >= 0) {
						SubscriptionImpl subscriptionImpl = typeSubscriptions.get(indexOf);
						// we remove the server from the list
						subscriptionImpl.getServers().remove(subscriptions.getServer());
						// if no servers remain interested in the subscription, we remove it alltogether
						if (subscriptionImpl.getServers().isEmpty()) {
							typeSubscriptions.remove(indexOf);
						}
					}
					// if we removed the last of the type subscriptions, remove it
					if (typeSubscriptions.isEmpty()) {
						subscriptionsByType.remove(subscriptionToRemove.getTypeId());
					}
				}
			}
			// update the map
			this.subscriptionsByType = subscriptionsByType;
		}
		
		// update our map
		if (subscriptions.getSubscriptions() == null || subscriptions.getSubscriptions().isEmpty()) {
			subscriptionsByServer.remove(subscriptions.getServer());
		}
		else {
			subscriptionsByServer.put(subscriptions.getServer(), subscriptions);
		}
	}
	
	// you subscribe to a type/query on a topic (only default topic atm)
	// once triggered, you want a service to be called with a particular input
	// this service must also implement a specification but it can add custom input to that with the service input
	// for example you might want to capture the websocket id that initiated the subscription in the service input
	// the service must also send a boolean back to indicate whether or not it is still active
	// unless it explicitly states that it is still active, its subscription will be terminated
	// spec: nabu.misc.broadcast.specs.subscriber
	public void subscribe(String typeId, String query, String topicId, String serviceId, Object serviceInput) {
		// TODO
		// important: must make a new instance of the input and map by key
		// we don't want to be manipulating the same object over and over again (with the data etc)
		
		// TODO: unsubscribe if active != true _or_ an exception occurred
	}
	
	// publish an object
	@SuppressWarnings("unchecked")
	public List<String> publish(Object instance) {
		if (instance == null) {
			throw new IllegalArgumentException("Can't publish null");
		}
		if (!(instance instanceof ComplexContent)) {
			instance = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(instance);
		}
		// not castable to complex
		if (instance == null) {
			throw new IllegalArgumentException("Can only publish complex content");
		}
		ComplexContent content = ((ComplexContent) instance);
		ComplexType type = content.getType();
		// we need a type id
		if (!(type instanceof DefinedType)) {
			throw new IllegalArgumentException("Can only publish defined types");
		}
		String typeId = ((DefinedType) type).getId();
		
		// we take a local copy of the reference, in case the subscription list is updated as we are working
		Map<String, List<SubscriptionImpl>> subscriptionsByType = this.subscriptionsByType;
		
		// the topics this should be broadcast on
		List<String> topicIds = new ArrayList<String>();
		List<SubscriptionImpl> subscriptions = subscriptionsByType.get(typeId);
		BroadcastMessage message = null;
		// if we have subscriptions, we might be interested
		if (subscriptions != null && !subscriptions.isEmpty()) {
			for (SubscriptionImpl subscription : subscriptions) {
				boolean interested = true;
				// if we have a query, check it
				if (subscription.getQuery() != null && !subscription.getQuery().trim().isEmpty()) {
					try {
						Object variable = getVariable(content, subscription.getQuery());
						if (variable == null) {
							variable = false;
						}
						else if (!(variable instanceof Boolean)) {
							variable = ConverterFactory.getInstance().getConverter().convert(variable, Boolean.class);
						}
						interested = (Boolean) variable;
					}
					catch (Exception e) {
						logger.error("Could not evaluate subscription: " + subscription.getQuery(), e);
						interested = false;
					}
				}
				if (interested) {
					if (message == null) {
						message = new BroadcastMessage();
						message.setTypeId(typeId);
					}
					message.getSubscriptionIds().add(subscription.getId());
					if (!topicIds.contains(subscription.getTopicId())) {
						topicIds.add(subscription.getTopicId());
					}
				}
			}
		}
		// if we are interested at all, let's do this!
		if (message != null && !topicIds.isEmpty()) {
			JSONBinding binding = new JSONBinding(type, Charset.forName("UTF-8"));
			ByteArrayOutputStream output = new ByteArrayOutputStream();
			try {
				binding.marshal(output, content);
			}
			catch (Exception e) {
				throw new RuntimeException("Could not marshal the content", e);
			}
			// we set the content
			message.setContent(new String(output.toByteArray(), Charset.forName("UTF-8")));
			for (String topicId : topicIds) {
				ClusterTopic<BroadcastMessage> targetTopic = cluster.topic("messaging." + topicId);
				targetTopic.publish(message);
			}
		}
		return message == null ? null : message.getSubscriptionIds();
	}

	private Map<String, TypeOperation> analyzedOperations = new HashMap<String, TypeOperation>();

	private ClusterInstance cluster;
	
	private Object getVariable(ComplexContent pipeline, String query) throws ServiceException {
		VariableOperation.registerRoot();
		try {
			return getOperation(query).evaluate(pipeline);
		}
		catch (Exception e) {
			throw new ServiceException("VM-13", "Could not get '" + query + "' from pipeline", e);
		}
		finally {
			VariableOperation.unregisterRoot();
		}
	}
	
	private TypeOperation getOperation(String query) throws ParseException {
		if (!analyzedOperations.containsKey(query)) {
			synchronized(analyzedOperations) {
				if (!analyzedOperations.containsKey(query))
					analyzedOperations.put(query, (TypeOperation) new PathAnalyzer<ComplexContent>(new TypesOperationProvider()).analyze(QueryParser.getInstance().parse(query)));
			}
		}
		return analyzedOperations.get(query);
	}
}
