package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.*;

import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

public class StorageSys implements StorageSystem {
    //Each device and component has its own information, specifying its current state.
    private final HashMap<DeviceId, DevData> deviceInformation;
    private final HashMap<ComponentId, CompData> componentInformation;

    //Below hashmaps are a graph representation, needed for DFS
    private final HashMap<DeviceId, LinkedList<ComponentTransfer>> waitingTransfers;
    private final HashMap<DeviceId, LinkedList<Semaphore>> waitingTransfersSemaphores;

    //Mutex for protection of the data, needed while checking if transfer is correct, is possible to execute,
    //searching for a cycle, or updating data at the end of a transfer.
    private final Semaphore mutex;

    public StorageSys(Map<DeviceId, Integer> deviceTotalSlots,
                      Map<ComponentId, DeviceId> componentPlacement) throws IllegalArgumentException {
        //At first, we have to check if arguments are correct.
        if (deviceTotalSlots.isEmpty()){
            throw new IllegalArgumentException("Created system has 0 devices.");
        }
        this.deviceInformation = new HashMap<>();
        this.waitingTransfers = new HashMap<>();
        this.waitingTransfersSemaphores = new HashMap<>();
        for (DeviceId devId : deviceTotalSlots.keySet()) {
            if (devId == null){
                throw new IllegalArgumentException("DeviceId cannot be null.");
            }
            if (deviceTotalSlots.get(devId) <= 0 || deviceTotalSlots.get(devId) == null){
                throw new IllegalArgumentException("Device " + devId + " has 0 available slots.");
            }
            deviceInformation.put(devId, new DevData(deviceTotalSlots.get(devId)));
            waitingTransfers.put(devId, new LinkedList<>());
            waitingTransfersSemaphores.put(devId, new LinkedList<>());
        }

        this.componentInformation = new HashMap<>();
        for (ComponentId compId : componentPlacement.keySet()) {
            if (compId == null){
                throw new IllegalArgumentException("ComponentId cannot be null");
            }
            DeviceId devId = componentPlacement.get(compId);
            if (devId == null){
                throw new IllegalArgumentException("Component " + compId + " is assigned to a device with null DeviceId");
            }
            if (!deviceInformation.containsKey(devId)){
                throw new IllegalArgumentException("Component " + compId + "is assigned to a device " + devId + "that does not exist in the system.");
            }
            int pos = deviceInformation.get(devId).reserveSlot();
            if (pos == -1){
                throw new IllegalArgumentException("Device " + devId + " has initially too much components assigned to it.");
            }
            deviceInformation.get(devId).acquireSlot(pos);
            deviceInformation.get(devId).initFreeSpaces();
            componentInformation.put(compId, new CompData(devId, pos));
        }

        this.mutex = new Semaphore(1);
    }

    @Override
    public void execute(ComponentTransfer transfer) throws TransferException {
        try {
            mutex.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption", e);
        }
        //We have to check if transfer is correct.
        checkIfCorrect(transfer);
        //Then we try to execute it.
        reserveDevices(transfer);
    }

    //Checking all possible wrong transfer conditions.
    public void checkIfCorrect(ComponentTransfer transfer) throws TransferException {
        if ((transfer.getSourceDeviceId() == null && transfer.getDestinationDeviceId() == null)) {
            throw new IllegalTransferType(transfer.getComponentId());
        }else if (transfer.getSourceDeviceId() != null && !deviceInformation.containsKey(transfer.getSourceDeviceId())){
            throw new DeviceDoesNotExist(transfer.getSourceDeviceId());
        }else if (transfer.getDestinationDeviceId() != null && !deviceInformation.containsKey(transfer.getDestinationDeviceId())){
            throw new DeviceDoesNotExist(transfer.getDestinationDeviceId());
        }else if (transfer.getSourceDeviceId() == null && componentInformation.containsKey(transfer.getComponentId())){
            throw new ComponentAlreadyExists(transfer.getComponentId(), transfer.getDestinationDeviceId());
        }else if (transfer.getSourceDeviceId() != null && (!componentInformation.containsKey(transfer.getComponentId()) || !componentInformation.get(transfer.getComponentId()).getSrcDevId().equals(transfer.getSourceDeviceId()))){
            throw new ComponentDoesNotExist(transfer.getComponentId(), transfer.getSourceDeviceId());
        }else if (componentInformation.containsKey(transfer.getComponentId()) && (transfer.getDestinationDeviceId() != null) && (transfer.getDestinationDeviceId().equals(componentInformation.get(transfer.getComponentId()).getSrcDevId()))){
            throw new ComponentDoesNotNeedTransfer(transfer.getComponentId(), transfer.getDestinationDeviceId());
        }else if (componentInformation.containsKey(transfer.getComponentId()) && componentInformation.get(transfer.getComponentId()).isOperatedOn()){
            throw new ComponentIsBeingOperatedOn(transfer.getComponentId());
        }
    }


    public void reserveDevices(ComponentTransfer transfer) {
        DeviceId destDevId = transfer.getDestinationDeviceId();
        DeviceId srcDevId = transfer.getSourceDeviceId();
        //If transfer is correct, and it's component is not in the componentInformation map, than it is an 'adding' transfer.
        //We put it there only once, if other transfer tries to add the same component, it will not be correct.
        if (!componentInformation.containsKey(transfer.getComponentId())){
            componentInformation.put(transfer.getComponentId(), new CompData(transfer.getDestinationDeviceId(), -1));
        }
        componentInformation.get(transfer.getComponentId()).operateOn();
        if (destDevId == null) {
            //Procedure for 'deleting' transfers.
            reserveForDeleting(transfer, srcDevId);
        } else {
            int pos = deviceInformation.get(destDevId).reserveSlot();
            if (pos == -1) {
                //If transfers cannot be executed immediately (there is no space on the source device), we check if it is a part of a cycle.
                checkForCycleOrWait(transfer, destDevId);
            } else {
                //If we can execute the transfer, we try to awake other possible transfers.
                LinkedList<ComponentTransfer> awakenTransfers = awakeTransfers(pos, transfer);
                ComponentTransfer firstTransfer = awakenTransfers.peekFirst();
                releaseFromQueue(awakenTransfers);
                mutex.release();
                prepareAndPerform(firstTransfer, destDevId); //First transfer is never null, because awakenTransfers always contains current transfer.
            }
        }
    }

    //Searching for a cycle, if it does not exist, transfer has to wait on a Semaphore.
    public void checkForCycleOrWait(ComponentTransfer transfer, DeviceId destDevId){
        LinkedList<ComponentTransfer> awakenTransfers = checkForCycle(transfer);
        if (!awakenTransfers.isEmpty()) {
            awakenTransfers = awakeTransfersInCycle(awakenTransfers);
            ComponentTransfer firstTransfer = awakenTransfers.peekFirst();
            ComponentTransfer lastTransfer = awakenTransfers.peekLast();
            releaseFromQueueForCycle(awakenTransfers);
            while (!awakenTransfers.isEmpty()) {
                transfer = awakenTransfers.removeFirst();
                if (!awakenTransfers.isEmpty()) {
                    ComponentTransfer peekNext = awakenTransfers.peekFirst();
                    int nextPosition = waitingTransfers.get(transfer.getSourceDeviceId()).indexOf(peekNext);
                    waitingTransfers.get(transfer.getSourceDeviceId()).remove(nextPosition);
                    waitingTransfersSemaphores.get(transfer.getSourceDeviceId()).remove(nextPosition).release();
                }
            }
            mutex.release();
            prepareAndPerformForCycles(lastTransfer, firstTransfer); //First transfer is never null, because awakenTransfers always contains current transfer.
        } else {
            addToQueueAndWait(transfer);
            prepareAndPerform(transfer, destDevId);
        }
    }

    //If transfer is deleting a component, it can be executed immediately, and then it can also wake up some transfers.
    public void reserveForDeleting(ComponentTransfer transfer, DeviceId srcDevId){
        int pos = componentInformation.get(transfer.getComponentId()).getSrcDevPos();
        ComponentTransfer nextTransfer = waitingTransfers.get(srcDevId).poll();
        deviceInformation.get(transfer.getSourceDeviceId()).increaseFreeSpaces(pos);
        if (nextTransfer != null){
            LinkedList<ComponentTransfer> awakenTransfers = awakeTransfers(pos, nextTransfer);
            //Since waitingTransferSemaphores has the same amount of elements as waitingTransfers, release won't produce exception.
            waitingTransfersSemaphores.get(transfer.getSourceDeviceId()).poll().release();
            releaseFromQueue(awakenTransfers);
        }
        mutex.release();
        prepareAndPerformForDeleting(transfer);
    }

    //Transfers that cannot be executed immediately, are added to queue, and are waiting on Semaphores.
    //If a transfer that just came makes them possible, they are moved to LinkedList 'awakenTransfers',
    // and after their information is properly set, they can be released from the 'waitingTransfers' queue and continue.
    public void releaseFromQueue(LinkedList<ComponentTransfer> awakenTransfers){
        while (!awakenTransfers.isEmpty()) {
            ComponentTransfer transfer = awakenTransfers.poll();
            if (!awakenTransfers.isEmpty()) {
                //Since waitingTransferSemaphores has the same amount of elements as waitingTransfers, release won't produce exception.
                waitingTransfersSemaphores.get(transfer.getSourceDeviceId()).poll().release();
            }
        }
    }

    //Same function as above, but transfers in cycles are not necessarily the longest waiting transfers ont their destination devices.
    //Because of that, we have to find their position in 'waitingTransfersQueue', and remove them, as well as their Semaphores.
    public void releaseFromQueueForCycle(LinkedList<ComponentTransfer> awakenTransfers){
        while (!awakenTransfers.isEmpty()) {
            ComponentTransfer transfer = awakenTransfers.removeFirst();
            if (!awakenTransfers.isEmpty()) {
                ComponentTransfer peekNext = awakenTransfers.peekFirst();
                int nextPosition = waitingTransfers.get(transfer.getSourceDeviceId()).indexOf(peekNext);
                waitingTransfers.get(transfer.getSourceDeviceId()).remove(nextPosition);
                waitingTransfersSemaphores.get(transfer.getSourceDeviceId()).remove(nextPosition).release();
            }
        }
    }

    //Prepare and perform for transfers that add, or move component.
    public void prepareAndPerform(ComponentTransfer transfer, DeviceId destDevId){
        transfer.prepare();
        componentInformation.get(transfer.getComponentId()).isPrepared();   //After preparing, we mark the component as prepared.
        //If component was moved from another device, we release the slot on previous device, as it is no longer occupied.
        if (transfer.getSourceDeviceId() != null){
            deviceInformation.get(transfer.getSourceDeviceId()).releaseSlot(componentInformation.get(transfer.getComponentId()).getSrcDevPos());
        }
        //Then, component has to wait for his future slot to be available.
        //It will happen, when the 'previous' transfer will end his 'prepare'.
        deviceInformation.get(destDevId).acquireSlot(componentInformation.get(transfer.getComponentId()).getDestDevPos());
        transfer.perform();
        //At last, we have to update information about transferred component.
        endTransfer(transfer);
    }

    //This function is similar to prepareAndPerform, but has 1 significant difference.
    //Before perform of the first transfer in the cycle, we have to wait for the 'last' transfer to end his 'prepare'.
    //We do not have to acquire the slot later, because prepare of the previous transfer has already ended.
    public void prepareAndPerformForCycles(ComponentTransfer lastTransfer, ComponentTransfer firstTransfer){
        firstTransfer.prepare();
        componentInformation.get(firstTransfer.getComponentId()).isPrepared();
        componentInformation.get(lastTransfer.getComponentId()).waitForPrepare();
        if (firstTransfer.getSourceDeviceId() != null){
            deviceInformation.get(firstTransfer.getSourceDeviceId()).releaseSlot(componentInformation.get(firstTransfer.getComponentId()).getSrcDevPos());
        }
        firstTransfer.perform();
        endTransfer(firstTransfer);
    }

    //Once more, this function is similar to prepareAndPerform, but with 1 difference.
    //Since deleting a component (if called with correct parameters) is always possible immediately,
    //we do not have to acquireSlot.
    public void prepareAndPerformForDeleting(ComponentTransfer transfer){
        transfer.prepare();
        componentInformation.get(transfer.getComponentId()).isPrepared();
        if (transfer.getSourceDeviceId() != null){
            deviceInformation.get(transfer.getSourceDeviceId()).releaseSlot(componentInformation.get(transfer.getComponentId()).getSrcDevPos());
        }
        transfer.perform();
        endTransfer(transfer);
    }

    //This function puts transfer in a queue 'waitingTransfers', and Semaphore for this transfer in similar queue.
    //Position of a specific transfer, and it's Semaphore is always the same in both queues.
    //If transfer is first in his queue, it's Semaphore is also first.
    public void addToQueueAndWait(ComponentTransfer transfer){
        DeviceId destDevId = transfer.getDestinationDeviceId();
        waitingTransfers.get(destDevId).add(transfer);
        Semaphore wait = new Semaphore(0);
        waitingTransfersSemaphores.get(destDevId).add(wait);
        //We are waiting in a queue assigned to a specific device, till other transfer will wake us up.
        mutex.release();
        try {
            wait.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption", e);
        }
    }

    //Awaking other transfers that became correct after currently analyzed transfer has arrived.
    public LinkedList<ComponentTransfer> awakeTransfers(int pos, ComponentTransfer transfer){
        return  setTransfersInformation(pos, null, transfer);
    }

    //Awaking transfers in a cycle.
    public LinkedList<ComponentTransfer> awakeTransfersInCycle(LinkedList<ComponentTransfer> chain){
        return setTransfersInformation(null, chain, null);
    }

    //Since function 'processTransfers' awakes transfers and sets their information (for example about destination device)
    //for both of the above cases, function 'setTransfersInformation' provides function 'processTransfers' with correct data.
    public LinkedList<ComponentTransfer> setTransfersInformation(Integer pos, LinkedList<ComponentTransfer> chain, ComponentTransfer transfer){
        LinkedList<ComponentTransfer> awakenTransfers = new LinkedList<>();
        ComponentTransfer nextTransfer;
        int destPosition;
        if (chain != null){
            nextTransfer = chain.removeLast();
        }else{
            nextTransfer = transfer;
        }
        DeviceId destDevId = nextTransfer.getDestinationDeviceId();
        DeviceId srcDevId = nextTransfer.getSourceDeviceId();
        if (pos == null){
            destPosition = componentInformation.get(chain.peekFirst().getComponentId()).getSrcDevPos(); //Chain will never be empty, because it always has at least 2 elements for the cycle.
        }else{
            destPosition = pos;
        }
        return processTransfers(srcDevId, destDevId, nextTransfer, destPosition, awakenTransfers, chain);
    }

    //Function that awakes transfers, and sets all necessary information for transfer before it can be prepared.
    //It wakes up transfers as long as there hasn't been a cycle, or we found a transfer that had sourceDeviceId == null.
    public LinkedList<ComponentTransfer> processTransfers(DeviceId srcDevId, DeviceId destDevId, ComponentTransfer nextTransfer, int destPosition, LinkedList<ComponentTransfer> awakenTransfers, LinkedList<ComponentTransfer> chain){
        do {
            srcDevId = nextTransfer.getSourceDeviceId();
            componentInformation.get(nextTransfer.getComponentId()).setDestDevPos(destPosition);
            componentInformation.get(nextTransfer.getComponentId()).setDestDevId(nextTransfer.getDestinationDeviceId());
            //Also it decreases/increases available spaces on destination/source device.
            if (nextTransfer.getDestinationDeviceId() != null){
                deviceInformation.get(nextTransfer.getDestinationDeviceId()).decreaseFreeSpaces(destPosition);
            }
            if (nextTransfer.getSourceDeviceId() != null){
                deviceInformation.get(nextTransfer.getSourceDeviceId()).increaseFreeSpaces(componentInformation.get(nextTransfer.getComponentId()).getSrcDevPos());
            }
            awakenTransfers.add(nextTransfer);
            if (srcDevId != null && !srcDevId.equals(destDevId)){
                destPosition = componentInformation.get(nextTransfer.getComponentId()).getSrcDevPos();
                if (chain == null){
                    nextTransfer = waitingTransfers.get(srcDevId).poll();
                }else{
                    nextTransfer = chain.removeLast();
                }
            }
        } while (srcDevId != null && !srcDevId.equals(destDevId) && nextTransfer != null);
        //The result of this function is a list of transfers that are now ready to be 'prepared'.
        return awakenTransfers;
    }

    //This function updates component information after transfer has ended it's 'perform'.
    public void endTransfer(ComponentTransfer transfer){
        try {
            mutex.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption", e);
        }
        //Component changes it's device, and is no longer operated on.
        if (transfer.getDestinationDeviceId() != null){
            componentInformation.get(transfer.getComponentId()).endOperation();
            componentInformation.get(transfer.getComponentId()).changePosition();
        }else{
            //If it was a transfer that deleted a component, we simply remove it as it no longer exists.
            componentInformation.remove(transfer.getComponentId());
        }
        mutex.release();
    }

    //Function checking if there is a cycle in a graph represented by graph 'waitingTransfers'.
    //We interpret waiting transfers as edges, and devices as vertexes.
    public LinkedList<ComponentTransfer> checkForCycle(ComponentTransfer transfer){
        //Initializing necessary information, such as Map determining if a vertex has already been visited.
        HashMap<DeviceId, LinkedList<ComponentTransfer>> adjacency = waitingTransfers;
        HashMap<DeviceId, Boolean> visited = new HashMap<>();
        for (DeviceId devId : adjacency.keySet()){
            visited.put(devId, false);
        }
        LinkedList<ComponentTransfer> stack = new LinkedList<>();
        LinkedList<ComponentTransfer> result = new LinkedList<>();
        if (DFScycle(adjacency, transfer, transfer, stack, visited)){
            result = stack;
        }
        return result;
    }

    //Using DFS to determine if graph has a cycle.
    public boolean DFScycle(HashMap<DeviceId, LinkedList<ComponentTransfer>> graph, ComponentTransfer firstTransfer, ComponentTransfer currentTransfer, LinkedList<ComponentTransfer> stack, HashMap<DeviceId, Boolean> visited){
        ComponentTransfer nextTransfer;
        LinkedList<ComponentTransfer> neighbours;
        visited.put(currentTransfer.getDestinationDeviceId(), true);
        stack.push(currentTransfer);
        if(currentTransfer.getSourceDeviceId() != null){
            neighbours = new LinkedList<>(graph.get(currentTransfer.getSourceDeviceId())); //List of neighbours of the current vertex, we will visit all of them.
            while(!neighbours.isEmpty()){
                nextTransfer = neighbours.peekFirst();
                if (nextTransfer.getSourceDeviceId() != null) {
                    //If current transfer is the same as starting transfer, we found a cycle.
                    if (nextTransfer.getSourceDeviceId().equals(firstTransfer.getDestinationDeviceId())) {
                        stack.push(nextTransfer);
                        return true;
                    }
                    //Otherwise, if there is a vertex that hasn't been visited yet, and we will achieve the starting transfer if we move to it, we also found a cycle.
                    if (!visited.get(nextTransfer.getSourceDeviceId()) && DFScycle(graph, firstTransfer, nextTransfer, stack, visited)) {
                        return true;
                    }
                }
                neighbours.poll();
            }
        }
        stack.pop();
        return false;
    }
}
