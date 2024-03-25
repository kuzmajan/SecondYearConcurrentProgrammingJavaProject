package cp2023.solution;

import java.util.concurrent.Semaphore;

/*
Device has its own information describing its current state.
FreeSpaces: counts all free spaces, including those where there still is a component, but is currently transferred, and soon will be removed.
Semaphore[] spaces: transfers wait there for the spaces to be completely free.
Boolean stateOfSpaces: tells if a places on this device is available.
 */

public class DevData {
    private int size;
    private int freeSpaces;
    private Semaphore[] spaces;
    private boolean[] stateOfSpaces;

    private static final boolean CAN_BE_RESERVED = true;
    private static final boolean CANT_BE_RESERVED = false;

    public DevData(int size){
        this.size = size;
        this.freeSpaces = size;
        this.spaces = new Semaphore[size];
        this.stateOfSpaces = new boolean[size];
        for (int i = 0; i < size; i++){
            spaces[i] = new Semaphore(1);
            stateOfSpaces[i] = CAN_BE_RESERVED;
        }
    }

    //Trying to reserve a slot, returns a specific position on the device, or -1 if device is full.
    public int reserveSlot(){
        int result = -1;
        if (freeSpaces > 0){
            for (int i = 0; i < size; i++) {
                if (stateOfSpaces[i] == CAN_BE_RESERVED) {
                    stateOfSpaces[i] = CANT_BE_RESERVED;
                    return i;
                }
            }
        }
        return result;
    }

    //Increases free spaces, also updates the state of specific place.
    public void increaseFreeSpaces(int pos){
        freeSpaces++;
        stateOfSpaces[pos] = CAN_BE_RESERVED;
    }

    //Wakes up transfer waiting on a specific position.
    public void releaseSlot(int pos){
        spaces[pos].release();
    }

    //Decreases free spaces, updates the state of specific place.
    public void decreaseFreeSpaces(int pos){
        freeSpaces--;
        stateOfSpaces[pos] = CANT_BE_RESERVED;
    }

    //While initializing device, this function reserves a place for a component.
    public void initFreeSpaces(){
        freeSpaces--;
    }

    //If transfer wants to acquire specific place, it has to wait on a semaphore.
    public void acquireSlot(int pos){
        try {
            spaces[pos].acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("panic: unexpected thread interruption", e);
        }
    }
}
