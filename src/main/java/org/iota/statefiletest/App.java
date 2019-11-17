package org.iota.statefiletest;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.StateDiff;
import com.iota.iri.model.TransactionHash;
import com.iota.iri.model.persistables.Milestone;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import com.iota.iri.utils.Pair;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ){
        try {
            new App();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static final String DB1 = "/mnt/volume/m20db/mainnetdb";
    private static final String DB2 = "/mnt/volume/mirror01db/mainnetdb";

    private static final String LOG = "/home/log";
    
    RocksDBPersistenceProvider m20, mirror01;
    Tangle mTangle, mirrorTangle;
    
    public App() throws Exception {
        m20 = new RocksDBPersistenceProvider(
                DB1, LOG, 1, Tangle.COLUMN_FAMILIES,
                Tangle.METADATA_COLUMN_FAMILY);
        
        mTangle = new Tangle();
        mTangle.addPersistenceProvider(m20);
        mTangle.init();
        
        mirror01 = new RocksDBPersistenceProvider(
                DB2, LOG, 1, Tangle.COLUMN_FAMILIES,
                Tangle.METADATA_COLUMN_FAMILY);
        mirrorTangle = new Tangle();
        mirrorTangle.addPersistenceProvider(mirror01);
        mirrorTangle.init();
        
        go2();
    }
    
    private void go2() throws Exception {
        System.out.println("Statediff count: ");
        System.out.println("m20: " + m20.count(StateDiff.class));
        System.out.println("mirror01: " + mirror01.count(StateDiff.class));

        System.out.println("Milestone count: ");
        System.out.println("m20: " + m20.count(Milestone.class));
        System.out.println("mirror01: " + mirror01.count(Milestone.class));

        System.out.println("Milestone start: ");
        System.out.println("m20: " + ((IntegerIndex)m20.first(Milestone.class, IntegerIndex.class).low).getValue());
        System.out.println("mirror01: " + ((IntegerIndex)mirror01.first(Milestone.class, IntegerIndex.class).low).getValue());
        
        Pair<Indexable, Persistable> first = m20.first(StateDiff.class, TransactionHash.class);
        int i =0, failed =0;
        try {
            while (first  != null && first.low != null) { //first.low null means end of line :(
                try {
                    if (mirror01.exists(StateDiff.class, first.low)) {
                        if (!checkState(((Hash)first.low))) {
                            System.out.println("Oh oh!!!");
                        } else {
                            i++;
                        }
                    } else {
                        failed++;
                    }
                } catch (Exception e) {
                    System.out.println("mirror01 does not have " + first.low);
                    e.printStackTrace();
                    failed++;
                }
                first = m20.next(StateDiff.class, first.low);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("final result: " + i);
        }
        System.out.println("All done! " + i + "(" + failed + "), total: " + (i + failed));
    }

    private void go() throws Exception {
        Pair<Indexable, Persistable> first = m20.first(Milestone.class, IntegerIndex.class);
        int msIndex = ((Milestone)first.hi).index.getValue();

        System.out.println("Starting at " + msIndex);
        

        Pair<Indexable, Persistable> mirrorFirst = mirror01.first(Milestone.class, IntegerIndex.class);
        int mirrorIndex = ((Milestone)mirrorFirst.hi).index.getValue();
        System.out.println("Mirror lowest: " + mirrorIndex);
        
        while (!mirror01.exists(Milestone.class, first.low)) {
            first = m20.next(Milestone.class, first.low);
            msIndex = ((Milestone)first.hi).index.getValue();
        }
        
        System.out.println("both nodes start at:  " + msIndex);
        System.out.println(first);

        System.out.println("Statediff count: ");
        System.out.println("m20: " + m20.count(StateDiff.class));
        System.out.println("mirror01: " + mirror01.count(StateDiff.class));
        
        // Both have the index
        MilestoneViewModel msHash;
        int start = msIndex;
        int failed = 0;
        while ((msHash = MilestoneViewModel.get(mTangle, msIndex)) != null) {
            if (!checkState(msHash.getHash())) {
                //System.out.println("Failed check at " + msIndex + "(" + msHash.getHash() + ")");
                failed++;
            }
            
            msIndex++;
        }
        
        System.out.println("Failed " + failed + " times!");
        System.out.println("Total checks: " + (msIndex - start));
    }

    private boolean checkState(Hash msHash) throws Exception {

        StateDiffViewModel stateDiff = StateDiffViewModel.load(mTangle, msHash);
        StateDiffViewModel stateDiff2 = StateDiffViewModel.load(mirrorTangle, msHash);
        
        boolean ok = true;
        if (stateDiff.getDiff() == null || stateDiff2.getDiff() == null) {
            if (stateDiff.getDiff() == null && stateDiff2.getDiff() == null) {
                System.out.println("Missing statediff on both databases!");
            } else {
                System.out.println("Missing statediff for " + (stateDiff.getDiff() == null ? "mTangle" : "mirrorTangle"));
            }
            return false;
        }
        
        if (!stateDiff.getDiff().equals(stateDiff2.getDiff())){
            System.out.println("Found broken statediff for " + msHash);
            Map<Hash, Long> newDiff = new HashMap<>(stateDiff2.getDiff());
            for (Entry<Hash, Long> e : stateDiff.getDiff().entrySet()) {
                if (newDiff.remove(e.getKey(), e.getValue())) {
                    System.out.println("mirrorTangle did not contain " + e);
                }
            }
            for (Entry<Hash, Long> e : newDiff.entrySet()) {
                System.out.println("mTangle did not contain " + e);
            }
            ok = false;
        }
        return ok;
    }
}




























