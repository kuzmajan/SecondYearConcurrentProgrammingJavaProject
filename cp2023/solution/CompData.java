package cp2023.solution;

import cp2023.base.DeviceId;

import java.util.concurrent.Semaphore;

/*
Component has its own information describing its current state.
The information stored here is:
-Source and destination device id,
-Position on these devices,
-Boolean describing if the component is currently operated on,
-Semaphore prepared, needed to determine if the component has already been prepared, when it comes to correct execution of cycle.
 */

public class CompData {
    private DeviceId srcDevId;
    private DeviceId destDevId;
    private int srcDevPos;
    private int destDevPos;
    private Semaphore prepared;
    private boolean isOperatedOn;

    public CompData(DeviceId devId, int srcDevPos){
        this.srcDevId = devId;
        this.srcDevPos = srcDevPos;
        this.destDevPos = -1;
        this.destDevId = null;
        this.prepared = new Semaphore(0);
    }

    public DeviceId getSrcDevId(){
        return srcDevId;
    }

    //When component is prepared, it enables the next transfer in a cycle to continue.
    public void isPrepared(){
        prepared.release();
    }

    public void waitForPrepare(){
        try {
            prepared.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void setDestDevPos(int i){
        destDevPos = i;
    }

    public void setDestDevId(DeviceId devId){
        destDevId = devId;
    }

    public int getSrcDevPos(){
        return srcDevPos;
    }

    public int getDestDevPos(){
        return destDevPos;
    }

    public boolean isOperatedOn(){
        return isOperatedOn;
    }

    public void operateOn(){
        isOperatedOn = true;
    }

    public void endOperation(){
        isOperatedOn = false;
    }

    public void changePosition(){
        srcDevId = destDevId;
        srcDevPos = destDevPos;
    }
}
