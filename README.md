# Project 5: Lock Manager

## API Overview
You will be implementing the logic for table and page-level locking.
To that end, we have created a high-level API for the LockManager,
Transaction, and Request objects that are managed by a single-threaded service.
Requests to these objects are handled sequentially and atomically by that service. 
(In many database systems, these objects are shared across database engine
threads, and hence require additional logic to manage their concurrent data
structures. We have abstracted that problem away for you.)

* Transactions are represented through the `Transaction` class.
* There are two types of resources (see the `Resource` class) on which transactions can obtain locks: 
tables (see the `Table` class) and pages (see the `Page` class). Tables consist of 1 of more pages. 
* Each `Request` object stores
    * the transaction that made the request
    * the type of lock requested
        * The locks you will need to support are `LockType.S`, `LockType.X`, `LockType.IS`, and `LockType.IX`.
          You will not need to support SIX locks.
        * Remember that since tables consist of pages, transactions can request intent locks as well as regular locks on a table.
          A transaction cannot request an intent lock on a page.
* The `LockManager` keeps track of the lock information for each resource using
  `ResourceLock` objects.
* Each `ResourceLock` object stores
    * a list of `Request` objects that represent which transactions own which type of lock on this resource
    * a queue of waiting `Request` objects 
    
## Acquire Lock
When a transaction `T` tries to acquire a lock on
a resource, either it is granted the lock or it gets added to the back of the FIFO queue
for the lock and the transaction is blocked. More specifically:

* If `T`'s lock request is _compatible_ with the resource's lock, it is added to the list of owners for the lock.
    * Concretely, you need to make a `Request` object and add it to the list of the lock's `lockOwners`.
    * A request is considered compatible on a resource if it is compatible with all the
requests that are currently granted on the resource based on the lock comparability matrix that you should 
have seen in lecture. We recommend you directly implement this matrix somewhere in your code, but you are not
required to do so.
    * We have provided an unimplemented `LockManager#compatible` helper method that checks to see if a lock request is
compatible. We encourage you to implement and use this helper method, but it is not required (the tests do not call this function directly).

* If `T`'s lock request is not compatible, `T` is placed on a FIFO queue of transactions
that are waiting to acquire the lock.
    * Concretely, you need to make a `Request` object and add it to the back of the lock's `requestersQueue`.
    * Make sure to call `Transaction#sleep` to update the status of `T` to `Transaction.Status.Waiting`.
      In a real database system this would cause the current thread to suspend execution
      for a specified period.

Note that we prioritize lock upgrades. This means that if a transaction is requesting a lock
upgrade (S to an X lock) and currently owns an S lock, if we can perform the
upgrade immediately (based on the compatibility matrix), then we do. Otherwise, we "prioritize" the
upgrade by placing it at the front of the queue. You DO NOT have to worry about IS to IX upgrades. 
We will not test this case.

Throw an `IllegalArgumentException` in any of the following error cases:
* If a blocked transaction calls acquire
* If a transaction requests a lock that it already holds
* If a transaction that currently holds an X lock on a resource requests an S lock on the same resource (downgrade)
* If a transaction that currently holds an IX lock on a table requests an IS lock the same table (downgrade)
* If a transaction requests an intent lock on a page
* If a transaction requests an S or X lock on a page without having an appropriate intent lock on the parent table.

## Release Lock

* The transaction should release any lock it contains on the resource.
* The set of mutually compatible requests from the beginning of the lock's `requestersQueue`
should be granted and the corresponding transaction should be woken up. 
* We have provided an unimplemented `LockManager#promote` helper method that will grant mutually
compatible lock requests for the resource from the FIFO queue. We encourage you to implement and
use this helper method, but you are free not to if you do not want to (the tests do not call this function
directly). You are also free to call `LockManager#compatible` in this part if you wish to.

Throw an `IllegalArgumentException` in any of the following error cases:
* If a blocked transaction calls release
* If the transaction doesn't hold any of the four possible lock types on this resource
* If a transaction is trying to release a table level lock without having released all the locks for the pages of that table first

## Holds Lock

Checks if a given transaction holds a lock of a given type on a given
resource.
