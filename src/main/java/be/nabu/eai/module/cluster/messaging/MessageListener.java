package be.nabu.eai.module.cluster.messaging;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.nabu.eai.module.cluster.messaging.api.SubscriptionSubscriber;
import be.nabu.eai.repository.RepositoryThreadFactory;
import be.nabu.eai.server.Server;
import be.nabu.eai.server.api.ServerListener;
import be.nabu.libs.cluster.api.ClusterInstance;
import be.nabu.libs.cluster.api.ClusterMember;
import be.nabu.libs.cluster.api.ClusterMembershipListener;
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
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.binding.api.Window;
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

	private ForkJoinPool pool;
	
	// synchronize every minute
	private static final long SYNCHRONIZATION_INTERVAL = 1000l * 60;
	// if we miss 5 synchronization windows, we consider the original subscriber lost and unsubscribe everything, if we set this to exactly 5, it is more likely to be 4 windows missed
	private static final long SYNCHRONIZATION_TIMEOUT = 1000l * 60 * 6;
	
	// if we have 1000 unprocessed, we want to start emitting warnings
	private static final long WARNING_LIMIT = Integer.parseInt(System.getProperty("messaging.warning", "1000"));
	// at this point we don't want to submit any more until it clears up
	// set to 0 if you don't want to cut it off
	private static final long CUTOFF_LIMIT = Integer.parseInt(System.getProperty("messaging.cutoff", "10000"));
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static MessageListener instance;
	
	// these are the subscriptions we check when new data arrives
	// we are not using volatile, slight delays in synchronization are OK
	private Map<String, List<SubscriptionImpl>> subscriptionsByType = new HashMap<String, List<SubscriptionImpl>>();
	
	// these are the subscriptions we check when a server updates its subscription list
	private Map<String, SubscriptionListImpl> subscriptionsByServer = new HashMap<String, SubscriptionListImpl>();
	
	// we do want this to be seen by all threads, especially the synchronizer
	private volatile Map<SubscriptionImpl, List<SubscriberImpl>> subscribers = new HashMap<SubscriptionImpl, List<SubscriberImpl>>();
	
	private Thread subscriptionSynchronizer, subscriptionPruner;
	private Server server;
	
	public static MessageListener getInstance() {
		return instance;
	}
	
	@Override
	public void listen(final Server server, HTTPServer httpServer) {
		this.server = server;
		instance = this;
		cluster = server.getCluster();
		
		// if a new server is added, we want him to be notified of our interests as soon as possible!
		cluster.addMembershipListener(new ClusterMembershipListener() {
			@Override
			public void memberRemoved(ClusterMember member) {
				// do nothing?
			}
			@Override
			public void memberAdded(ClusterMember member) {
				synchronizeOwnSubscriptions();
			}
		});
		
		// we listen to the subscription topic to keep subscriptions up to date
		ClusterTopic<SubscriptionListImpl> subscriptionTopic = cluster.topic("messaging.$subscriptions");
		subscriptionTopic.subscribe(new ClusterMessageListener<SubscriptionListImpl>() {
			@Override
			public void onMessage(SubscriptionListImpl message) {
				try {
					processClusterSubscriptionList(message);
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
					processClusterMessage(message);
				}
				catch (Exception e) {
					logger.error("Could not process heartbeat", e);
				}
			}
		});
		
		subscriptionSynchronizer = new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("Starting message subscription synchronization");
				try {
					while (true) {
						try {
							SubscriptionListImpl subscriptionListImpl = new SubscriptionListImpl();
							List<SubscriptionImpl> subscriptions = new ArrayList<SubscriptionImpl>();
							subscriptions.addAll(subscribers.keySet());
							subscriptionListImpl.setSubscriptions(subscriptions);
							subscriptionListImpl.setServer(server.getName());
							ClusterTopic<SubscriptionListImpl> subscriptionTopic = cluster.topic("messaging.$subscriptions");
							subscriptionTopic.publish(subscriptionListImpl);
							// if no on interrupted while processing, we go into a deep sleep
							if (!Thread.interrupted()) {
								Thread.sleep(SYNCHRONIZATION_INTERVAL);
							}
						}
						catch (InterruptedException e) {
							// clear the flag
							Thread.interrupted();
						}
					}
				}
				catch (Exception e) {
					logger.warn("Stopping message subscription synchronization", e);
				}
			}
		});
		subscriptionSynchronizer.setDaemon(true);
		subscriptionSynchronizer.setName("messaging-subscription-synchronizer");
		subscriptionSynchronizer.start();
		
		subscriptionPruner = new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("Starting message subscription pruner");
				try {
					while (true) {
						try {
							Date date = new Date();
							HashMap<String, SubscriptionListImpl> subscriptionsByServer;
							// we take a local copy in a synchronized block to not collide with the method below 
							synchronized(MessageListener.this) {
								subscriptionsByServer = new HashMap<String, SubscriptionListImpl>(MessageListener.this.subscriptionsByServer);
							}
							// we might be removing subscriptions from the global map, but the local instance will stay untouched
							for (Entry<String, SubscriptionListImpl> entry : subscriptionsByServer.entrySet()) {
								if (entry.getValue().getCreated().getTime() < date.getTime() - SYNCHRONIZATION_TIMEOUT) {
									logger.warn("Pruning message subscriptions for: " + entry.getKey());
									SubscriptionListImpl subscriptions = new SubscriptionListImpl();
									subscriptions.setServer(entry.getKey());
									// by setting an empty list, we will be unsubscribing from everything and deleting the entry from the map
									processClusterSubscriptionList(subscriptions);
								}
							}
							// if no on interrupted while processing, we go into a deep sleep
							if (!Thread.interrupted()) {
								Thread.sleep(SYNCHRONIZATION_INTERVAL);
							}
						}
						catch (InterruptedException e) {
							// clear the flag
							Thread.interrupted();
						}
					}
				}
				catch (Exception e) {
					logger.warn("Stopping message subscription pruner", e);
				}
			}
		});
		subscriptionPruner.setDaemon(true);
		subscriptionPruner.setName("messaging-subscription-pruner");
		subscriptionPruner.start();
		
		RepositoryThreadFactory threadFactory = new RepositoryThreadFactory(server.getRepository(), true);
		threadFactory.setName("cluster-messaging-processor");
		Integer poolSize = Integer.parseInt(System.getProperty("messaging.poolSize", "2"));
		// if we explicitly set it to size 0, it is unlimited. i wouldn't do that :|
		pool = new ForkJoinPool(poolSize, threadFactory, new Thread.UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				logger.error("Could not process incoming messaging data", e);
			}
		}, false);
		
		logger.info("Started listening to cluster messages with " + poolSize + " threads");
	}
	
	private void processClusterMessage(final BroadcastMessage message) {
		Map<SubscriptionImpl, List<SubscriberImpl>> subscribers;
		// we take a synchronized copy to avoid concurrency issues
		synchronized(this) {
			subscribers = new HashMap<SubscriptionImpl, List<SubscriberImpl>>(this.subscribers);
		}
		List<SubscriptionImpl> subscriptionsToUnsubscribe = new ArrayList<SubscriptionImpl>();
		List<SubscriberImpl> subscribersToUnsubscribe = new ArrayList<SubscriberImpl>();
		// TODO: unsubscribe if active != true _or_ an exception occurred
		// important: must make a new instance of the input and map by key
		// we don't want to be manipulating the same object over and over again (with the data etc)
		if (message.getSubscriptionIds() != null && !message.getSubscriptionIds().isEmpty()) {
			List<SubscriptionImpl> subscriptions = getSubscriptions(message.getSubscriptionIds(), subscribers);
			// the subscriptions might not have been aimed at this server, or it may already have been cancelled
			if (!subscriptions.isEmpty()) {
				DefinedType resolve = DefinedTypeResolverFactory.getInstance().getResolver().resolve(message.getTypeId());
				if (!(resolve instanceof ComplexType)) {
					logger.warn("Unsubscribing because type can not be resolved to a complex type: " + message.getTypeId());
					// we can never resolve them, let it go!
					subscriptionsToUnsubscribe.addAll(subscriptions);
				}
				else {
					try {
						JSONBinding binding = new JSONBinding((ComplexType) resolve, Charset.forName("UTF-8"));
						final ComplexContent content = binding.unmarshal(new ByteArrayInputStream(message.getContent().getBytes(Charset.forName("UTF-8"))), new Window[0]);
						for (final SubscriptionImpl subscription : subscriptions) {
							List<SubscriberImpl> list = subscribers.get(subscription);
							if (list != null && !list.isEmpty()) {
								for (final SubscriberImpl subscriber : list) {
									if (WARNING_LIMIT > 0 && pool.getQueuedSubmissionCount() > WARNING_LIMIT) {
										logger.warn("There are not enough resources to process the available messages");
									}
									if (CUTOFF_LIMIT > 0 && pool.getQueuedSubmissionCount() > CUTOFF_LIMIT) {
										logger.error("Message processing stopped until more resources become available");
									}
									else {
										pool.submit(new Runnable() {
											@Override
											public void run() {
												try {
													// if we are no longer interested, unsubscribe
													if (!subscriber.fire(server.getRepository(), subscription.getId() + "::" + subscriber.getId(), message.getTypeId(), content)) {
														unsubscribe(Arrays.asList(subscription), Arrays.asList(subscriber));
													}
												}
												catch (Exception e) {
													logger.warn("Subscriber threw exception, unsubscribing", e);
													unsubscribe(Arrays.asList(subscription), Arrays.asList(subscriber));
												}
											}
										});
									}
								}
							}
						}
					}
					// we don't want to stop listening at this point
					catch (Exception e) {
						logger.error("Could not process broadcast message", e);
					}
				}
			}
		}
		if (!subscriptionsToUnsubscribe.isEmpty()) {
			unsubscribe(subscriptionsToUnsubscribe, subscribersToUnsubscribe);
		}
	}
	
	private List<SubscriptionImpl> getSubscriptions(List<String> id, Map<SubscriptionImpl, List<SubscriberImpl>> subscribers) {
		List<SubscriptionImpl> subscriptions = new ArrayList<SubscriptionImpl>();
		for (SubscriptionImpl subscription : subscribers.keySet()) {
			if (id.contains(subscription.getId())) {
				subscriptions.add(subscription);
			}
		}
		return subscriptions;
	}
	
	synchronized private void processClusterSubscriptionList(SubscriptionListImpl subscriptions) {
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
	
	public void unsubscribe(String subscriptionId) {
		String[] split = subscriptionId.split("::");
		if (split.length != 2) {
			throw new IllegalArgumentException("Invalid subscription id");
		}
		List<SubscriptionImpl> subscriptions = getSubscriptions(Arrays.asList(split[0]), subscribers);
		if (!subscriptions.isEmpty()) {
			List<SubscriberImpl> subscribersToRemove = new ArrayList<SubscriberImpl>();
			for (SubscriptionImpl subscription : subscriptions) {
				List<SubscriberImpl> list = subscribers.get(subscription);
				if (list != null && !list.isEmpty()) {
					for (SubscriberImpl subscriber : list) {
						if (subscriber.getId().equals(split[1])) {
							subscribersToRemove.add(subscriber);
						}
					}
				}
			}
			if (!subscribersToRemove.isEmpty()) {
				unsubscribe(subscriptions, subscribersToRemove);
			}
		}
	}
	
	synchronized private void unsubscribe(List<SubscriptionImpl> subscriptions, List<SubscriberImpl> subscribers) {
		Map<SubscriptionImpl, List<SubscriberImpl>> allSubscribers = new HashMap<SubscriptionImpl, List<SubscriberImpl>>(this.subscribers);
		
		boolean changedSubscriptions = false;
		
		for (SubscriptionImpl subscription : subscriptions) {
			List<SubscriberImpl> list = allSubscribers.get(subscription);
			// if we removed at least one subscriber, we assume we are done for this subscription
			// if we don't, we assume you want to remove all subscribers for this subscription
			if (!list.removeAll(subscribers)) {
				list.clear();
			}
			// if no subscribers left, we are not interested anymore
			if (list.isEmpty()) {
				allSubscribers.remove(subscription);
				changedSubscriptions = true;
			}
		}
		
		this.subscribers = allSubscribers;
		if (changedSubscriptions) {
			synchronizeOwnSubscriptions();
		}
	}
	
	// you subscribe to a type/query on a topic (only default topic atm)
	// once triggered, you want a service to be called with a particular input
	// this service must also implement a specification but it can add custom input to that with the service input
	// for example you might want to capture the websocket id that initiated the subscription in the service input
	// the service must also send a boolean back to indicate whether or not it is still active
	// unless it explicitly states that it is still active, its subscription will be terminated
	// spec: nabu.misc.broadcast.specs.subscriber
	@SuppressWarnings("unchecked")
	public String subscribe(String typeId, String query, String topicId, String serviceId, Object serviceInput) {
		SubscriptionImpl subscription = new SubscriptionImpl();
		subscription.setTypeId(typeId);
		subscription.setQuery(query);
		subscription.setTopicId(topicId);
		
		SubscriberImpl subscriber = new SubscriberImpl();
		subscriber.setServiceId(serviceId);
		ComplexContent input;
		if (serviceInput == null) {
			input = null;
		}
		else if (serviceInput instanceof ComplexContent) {
			input = (ComplexContent) serviceInput;
		}
		else {
			input = ComplexContentWrapperFactory.getInstance().getWrapper().wrap(serviceInput);
			if (input == null) {
				throw new IllegalArgumentException("Could not wrap service input into a complex content");
			}
		}
		subscriber.setInput(input);
		
		boolean isNewSubscription = false;
		
		List<SubscriberImpl> list = subscribers.get(subscription);
		if (list == null) {
			synchronized(this) {
				list = subscribers.get(subscription);
				if (list == null) {
					list = new ArrayList<SubscriberImpl>();
					subscribers.put(subscription, list);
					isNewSubscription = true;
				}
			}
		}
		if (!list.contains(subscriber)) {
			list.add(subscriber);
		}

		// we only need to synchronize if the subscription is new
		// if its the tenth subscriber to the same subscription, we don't need to refresh immediately
		if (isNewSubscription) {
			synchronizeOwnSubscriptions();
		}
		
		// the unique combination that allows us to find it again
		return subscription.getId() + "::" + subscriber.getId();
	}
	
	private void synchronizeOwnSubscriptions() {
		subscriptionSynchronizer.interrupt();
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
	
	public SubscriptionSubscriber resolve(String subscriptionId) {
		SubscriptionSubscriberImpl result = new SubscriptionSubscriberImpl();
		String[] split = subscriptionId.split("::");
		if (split.length != 2) {
			throw new IllegalArgumentException("Invalid subscription id");
		}
		for (SubscriptionImpl subscription : new ArrayList<SubscriptionImpl>(subscribers.keySet())) {
			if (subscription.getId().equals(split[0])) {
				result.setSubscription(subscription);
				List<SubscriberImpl> list = subscribers.get(subscription);
				if (list != null) {
					for (SubscriberImpl subscriber : new ArrayList<SubscriberImpl>(list)) {
						if (subscriber.getId().equals(split[1])) {
							result.setSubscriber(subscriber);
							break;
						}
					}
				}
				break;
			}
		}
		return result;
	}

	public List<SubscriptionSubscriber> listAll() {
		List<SubscriptionSubscriber> list = new ArrayList<SubscriptionSubscriber>();
		for (SubscriptionImpl subscription : new ArrayList<SubscriptionImpl>(subscribers.keySet())) {
			List<SubscriberImpl> subscriptionSubscribers = subscribers.get(subscription);
			if (subscriptionSubscribers != null) {
				for (SubscriberImpl subscriber : new ArrayList<SubscriberImpl>(subscriptionSubscribers)) {
					SubscriptionSubscriberImpl result = new SubscriptionSubscriberImpl();
					result.setSubscription(subscription);
					result.setSubscriber(subscriber);
					list.add(result);
				}
			}
		}
		return list;
	}
}
