# Goal

The goal of this package is to allow memory-based data broadcasting to all servers.

The problem we are trying to solve:

While it has many potential uses (shared trace mode for example), the primary usecase now is dashboarding.
If you want to continually update your graphs based on the latest data you can either do long polling or push data.

Long polling is a tradeoff between data freshness and server load however. Especially for larger data sets, the queries can add some load which will have you put the poll interval lower.
This in turn makes it less usable.

If we look at something like events, that single data stream can account for multiple graphs in a single dashboard page as there are many conclusions to be drawn from the event data.
This means, with long polling, we are doing multiple potentially heavy queries on the same data set.

So what we want is an initial REST call to get history, then continually updating data via websockets.

However, in a cluster setup, a user might be listening on server 2 while the data is coming in on server 1. This module allows that data to be pushed to everyone.

Note that this package is focused on performance, not guarantees, all data streams are best effort, data may very well be dropped in edge cases.

# Implementation

What matters in any data subscription is the type of data and an optional query to filter what you are interested in.

Two approaches are possible:

- broadcast every instance of a particular type, let every server for himself determine whether or not they are interested
- have servers broadcast their subscriptions (so what they are interested in) and the server publishing first determines if anyone is interested

This second approach offers a lot of potential performance improvements:

1) Suppose you have 10 servers, 1 broadcasts a signal and anywhere from 1 to 10 servers has to evaluate their own rules to check if they are interested.
Especially in web applications, there is a high likelyhood of multiple servers being interested in the same data for different end users.
This evaluation, while fast, still takes an overhead. By centralizing the evaluation, we make sure every rule is executed only once, even in a 100 server setup.

2) Data has to be marshalled before sending and -crucially- unmarshalled before rules can be evaluated. So we always take the overhead of serialization even if we are not actually interested in the data instance.

3) If, in the end, we are only interested in a fraction of the data stream, we can severely reduce the chatter between servers by only sending data that at least someone is interested in. Especially when no listeners are active, no data will be sent, this allows you to just send a lot of data, knowing you won't overload the system simply by sending it.

The only downside to this approach is servers have to periodically emit their subscriptions, and especially when a server crashes it might take a while before his subscriptions are unsubscribed.
But due to the low occurence of such an event and the massive potential performance gains, this approach is worth it.