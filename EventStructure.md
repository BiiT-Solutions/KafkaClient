# Event Structure

This library defines the basic event structure that must be followed by any BiiT application. It will include some
contractual properties that can be required by any consumer/producer already deployed on a BiiT system.

### To parameter

If you want to specify a component that must receive this event, you can use this field. Any consumer only will process
this event if the `To` parameter is empty or match the consumer name.

### Replying

Sometimes, when an event is sent to a component to create or update an entity, the component returns the result or a
message.

### ReplayTo parameter

To get a reply for the request sent, the issuer should set the parameter `replyTo` to receive the result of the request.
If `replayTo` is not set the component will not reply the request.

### SessionId, MessageId and CorrelationId parameter

When a message is replayed, system will return a new message where correlationId will be the request messageId and
the sessionId will be the same sessionId as the request one.

This is useful to follow the flow of the messages as all the messages can be grouped by sessionId and sorted by
messageId and correlationId.

i.e.

###### Request Message

| Key           | Value                                  |
|---------------|----------------------------------------|
| sessionId     | 1723da1a-c6e7-4ef8-8816-25dc38f8366a   |
| messageId     | *5b2b285f-57a6-44b0-b9b5-6ecf0f172708* |
| correlationId | 96933086-5dcb-4426-abc4-130cd87b868d   |

###### Response Message

| Key           | Value                                    |
|---------------|------------------------------------------|
| sessionId     | 1723da1a-c6e7-4ef8-8816-25dc38f8366a     |
| messageId     | **c9ac7d58-e5b2-4873-93cd-6b602596563d** |
| correlationId | *5b2b285f-57a6-44b0-b9b5-6ecf0f172708*   |

When service receive the response message, it will know this response comes from the request message as its
correlationId is the same id as the messageId on the request message and both messages are on the same sessionId.

Each message sent will get a unique id to identify the message from the other ones.

### Subject/Label parameter

The event includes subject parameter to indicate the message purpose.

Currently, these are the values supported by this library.

| Action / Subject | Purpose                                        |
|------------------|------------------------------------------------|
| CREATE           | Request to create a entity                     |
| UPDATE           | Updates the entity requested                   |
| DELETE           | Deletes an entity                              |
| CREATED          | Indicates an entity has been created           |
| UPDATED          | Indicates an entity has been updated           |
| DELETED          | Indicates an entity has been deleted           |
| ENABLE           | Request to enable an entity                    |
| ENABLED          | Indicates an entity has been enabled           |
| DISABLE          | Request to disable an entity                   |
| DISABLED         | Indicates an entity has been disabled          |
| ERROR            | Notify an error on request received            |
| SUCCESS          | Notify request has been processed successfully |

### Content-Type

When body is set, content-type parameter needs to be set to indicates what kind of data is being sent so service can
manage it.

> **NOTE!** Please, use [IANA media-types](https://www.iana.org/assignments/media-types/media-types.xhtml#application)
> to set the proper value and keep services harmony under the standards.

### Custom Properties

Sometimes, we need to send extra data. In this case we can use custom properties to send extra-values to be processed.

This definition depends on the service and the action is notifying or requesting.

See documentation related to the desired service to know what is the properties needed or provided by their actions.

### Payload

The data that is sent. Usually an object as json, but depends on the content-type parameter.

# Simple Event Example
