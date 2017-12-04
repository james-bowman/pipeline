# pipeline

This code makes the following assumptions:

- Store is idempotent i.e. the same event can be delivered multiple times without any side effects
- ID values increase according to time of creation i.e. newer events will have higher ID numbers
- Previously published events will not be updated but will be published as new events with new IDs
