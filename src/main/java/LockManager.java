import java.util.*;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();

    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {
        // HW5: To do

        //If a transaction requests a lock that it already holds
        if (holds(transaction, resource, lockType)) {
            throw new IllegalArgumentException();
        }

        //If a transaction that currently holds an X lock on a resource requests an S lock on the same resource (downgrade) OR
        //If a transaction that currently holds an IX lock on a table requests an IS lock the same table (downgrade)
        //throw exception
        if (isDowngrade(transaction, resource, lockType)) {
            throw new IllegalArgumentException();
        }

        //If a blocked transaction calls acquire
        //throw exception
        if (Transaction.Status.Waiting == transaction.getStatus()) {
            throw new IllegalArgumentException();
        }

        //If a transaction requests an intent lock on a page
        //throw exception
        if (resource.getResourceType() == Resource.ResourceType.PAGE && (lockType == LockType.IX || lockType == LockType.IS)) {
            throw new IllegalArgumentException();
        }

        //If a transaction requests an S or X lock on a page without having an appropriate intent lock on the parent table.
        //throw exception
        if (lockType == LockType.S) {
            if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                Page p = (Page) resource;
                Resource test = p.getTable();
                if (resourceToLock.containsKey(test)) {
                    ResourceLock rl = resourceToLock.get(test);
                    ArrayList<Request> rList = rl.lockOwners;
                    boolean throw_needed = true;
                    for (Request r: rList) {
                        if (r.lockType == LockType.IS || r.lockType == LockType.IX) {
                            throw_needed = false;
                        }
                    }
                    if (throw_needed) {
                        throw new IllegalArgumentException();
                    }
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }
        if (lockType == LockType.X) {
            if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                Page p = (Page) resource;
                Resource test = p.getTable();
                if (resourceToLock.containsKey(test)) {
                    ResourceLock rl = resourceToLock.get(test);
                    ArrayList<Request> rList = rl.lockOwners;
                    boolean throw_needed = true;
                    for (Request r: rList) {
                        if (r.lockType == LockType.IS || r.lockType == LockType.IX) {
                            throw_needed = false;
                        }
                    }
                    if (throw_needed) {
                        throw new IllegalArgumentException();
                    }
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }

        //If T's lock request is compatible with the resource's lock, it is added to the list of owners for the lock
        if (compatible(resource, transaction, lockType)) {
            ResourceLock rl = null;
            if (!resourceToLock.containsKey(resource)) {
                rl = new ResourceLock();
            } else {
                rl = resourceToLock.get(resource);
                if (lockType == LockType.X) {
                    ArrayList<Request> remove = new ArrayList<>();
                    ArrayList<Request> lockRequests = this.resourceToLock.get(resource).lockOwners;
                    for (Request r : lockRequests) {
                        if ((r.transaction.equals(transaction)) && (r.lockType.equals(LockType.S))) {
                            remove.add(r);
                        }
                    }
                    for (Request r: remove) {
                        this.resourceToLock.get(resource).lockOwners.remove(r);
                    }
                }
            }
            rl.lockOwners.add(new Request(transaction, lockType));
            resourceToLock.put(resource, rl);
        } else {
            //Make sure to call Transaction#sleep to update the status of T to Transaction.Status.Waiting. In a real database system this would cause the current thread to suspend execution for a specified period
            transaction.sleep();
            //If T's lock request is not compatible, T is placed on a FIFO queue of transactions that are waiting to acquire the lock.
            if (!isUpgrade(transaction, resource, lockType)) {
                resourceToLock.get(resource).requestersQueue.add(new Request(transaction, lockType));
            } else {
                resourceToLock.get(resource).requestersQueue.add(0, new Request(transaction, lockType));
            }
        }
    }

    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param request the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType request) {
        // HW5: To do
        if (this.resourceToLock.containsKey(resource)) {
            ArrayList<Request> rList = this.resourceToLock.get(resource).lockOwners;
            for (Request r: rList) {
                LockType old = r.lockType;
                if (!lockComparabilityMatrix(old, request)) {
                    if (!(r.transaction.equals(transaction) && r.lockType == LockType.S && request == LockType.X)) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{

        //If a blocked transaction calls release
        //throw exception
        if (Transaction.Status.Waiting == transaction.getStatus()) {
            throw new IllegalArgumentException();
        }

        //If the transaction doesn't hold any of the four possible lock types on this resource
        //throw exception
        if (resourceToLock.containsKey(resource)) {
            ResourceLock rl = resourceToLock.get(resource);
            ArrayList<Request> rList = rl.lockOwners;
            boolean need_throw = true;
            for (Request r: rList) {
                if (r.transaction.equals(transaction) && r.lockType != null) {
                    need_throw = false;
                }
            }
            if (need_throw) {
                throw new IllegalArgumentException();
            }
        } else {
            throw new IllegalArgumentException();
        }

        //If a transaction is trying to release a table level lock without having released all the locks for the pages of that table first
        //throw exception
        if (resource.getResourceType() == Resource.ResourceType.TABLE) {
            Table t = (Table) resource;
            for (Page p: t.getPages()) {
                if (resourceToLock.containsKey((Resource) p)) {
                    ArrayList<Request> rList = resourceToLock.get((Resource) p).lockOwners;
                    for (Request r: rList) {
                        if (r.transaction.equals(transaction)) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
            }
        }

        // HW5: To do
        ArrayList <Request> removalList = new ArrayList<>();
        for (Request r: resourceToLock.get(resource).lockOwners) {
            if (r.transaction.equals(transaction)) {
                removalList.add(r);
            }
        }
        for (Request r: removalList) {
            resourceToLock.get(resource).lockOwners.remove(r);
        }
        promote(resource);
    }

    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {
         // HW5: To do
         ResourceLock rl =  resourceToLock.get(resource);
         while (rl.requestersQueue.size() != 0) {
             Request r = rl.requestersQueue.get(0);
             Transaction transaction = r.transaction;
             LockType lockType = r.lockType;
             if (compatible(resource, transaction, lockType)) {
                 transaction.wake();
                 acquire(transaction, resource, lockType);
                 rl.requestersQueue.remove(0);
             }
             else {
                 break;
             }
         }
         return;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        // HW5: To do
        if (!this.resourceToLock.containsKey(resource)) {
            return false;
        }
        ArrayList<Request> lockRequests = this.resourceToLock.get(resource).lockOwners;
        for (Request r: lockRequests) {
            if ((r.transaction.equals(transaction)) && (r.lockType.equals(lockType))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the lockType would be a downgrade.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if lock is a downgrade
     */
    public boolean isDowngrade(Transaction transaction, Resource resource, LockType lockType) {
        if (!this.resourceToLock.containsKey(resource)) {
            return false;
        }
        ArrayList<Request> lockRequests = this.resourceToLock.get(resource).lockOwners;
        if (lockType == LockType.S) {
            for (Request r : lockRequests) {
                if ((r.transaction.equals(transaction)) && (r.lockType.equals(LockType.X))) {
                    return true;
                }
            }
        }
        if (lockType == LockType.IS) {
            for (Request r : lockRequests) {
                if ((r.transaction.equals(transaction)) && (r.lockType.equals(LockType.IX))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if the lockType would be a upgrade.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if lock is a downgrade
     */
    public boolean isUpgrade(Transaction transaction, Resource resource, LockType lockType) {
        if (!this.resourceToLock.containsKey(resource)) {
            return false;
        }
        ArrayList<Request> lockRequests = this.resourceToLock.get(resource).lockOwners;
        if (lockType == LockType.X) {
            for (Request r : lockRequests) {
                if ((r.transaction.equals(transaction)) && (r.lockType.equals(LockType.S))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Simple implementation of comparability Matrix via series of if and elses.
     * Copied table from https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2008-r2/ms186396(v%3dsql.105)
     * @param request lockType in question
     * @param old original lockType
     * @return true if compatible
     */
    public boolean lockComparabilityMatrix(LockType old, LockType request) {
        if (request.equals(LockType.IS)) {
            if (old.equals(LockType.X)) {
                return false;
            }
            return true;
        }
        if (request.equals(LockType.S)) {
            if (old.equals(LockType.S) || old.equals(LockType.IS) ) {
                return true;
            }
            return false;
        }
        if (request.equals(LockType.IX)) {
            if (old.equals(LockType.IX) || old.equals(LockType.IS)) {
                return true;
            }
            return false;
        }
        if (request.equals(LockType.X)) {
            return false;
        }
        return false;
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }


    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}
