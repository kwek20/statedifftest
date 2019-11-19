package org.iota.statefiletest;

import java.util.Arrays;
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
    private static String DB1 = "/mnt/volume/db1/mainnetdb";
    private static String DB2 = "/mnt/volume/db2/mainnetdb";

    private static String LOG = "/home/log";
    
    public static void main( String[] args ){
        try {
            System.out.println(Arrays.toString(args));
            if (args.length >= 1) {
                DB1 = args[0];
            }if (args.length >= 2) {
                DB2 = args[1];
            }if (args.length >= 3) {
                LOG = args[2];
            }
            
            new App();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    RocksDBPersistenceProvider db1, db2;
    Tangle db1Tangle, db2Tangle;
    
    public App() throws Exception {
        db1 = new RocksDBPersistenceProvider(
                DB1, LOG, 1, Tangle.COLUMN_FAMILIES,
                Tangle.METADATA_COLUMN_FAMILY);
        
        db1Tangle = new Tangle();
        db1Tangle.addPersistenceProvider(db1);
        db1Tangle.init();
        
        db2 = new RocksDBPersistenceProvider(
                DB2, LOG, 1, Tangle.COLUMN_FAMILIES,
                Tangle.METADATA_COLUMN_FAMILY);
        db2Tangle = new Tangle();
        db2Tangle.addPersistenceProvider(db2);
        db2Tangle.init();
        
        System.out.println("db1: " + DB1);
        System.out.println("db2: " + DB2);
        System.out.println("log: " + LOG);
        go2();
    }
    
    private void go2() throws Exception {
        System.out.println("Statediff count: ");
        System.out.println("db1: " + db1.count(StateDiff.class));
        System.out.println("db2: " + db2.count(StateDiff.class));

        System.out.println("Milestone count: ");
        System.out.println("db1: " + db1.count(Milestone.class));
        System.out.println("db2: " + db2.count(Milestone.class));

        System.out.println("Milestone start: ");
        System.out.println("db1" + ((IntegerIndex)db1.first(Milestone.class, IntegerIndex.class).low).getValue());
        System.out.println("db2: " + ((IntegerIndex)db2.first(Milestone.class, IntegerIndex.class).low).getValue());
        
        Pair<Indexable, Persistable> first = db1.first(StateDiff.class, TransactionHash.class);
        int i =0, failed =0;
        try {
            while (first  != null && first.low != null) { //first.low null means end of line :(
                try {
                    if (db2.exists(StateDiff.class, first.low)) {
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
                first = db1.next(StateDiff.class, first.low);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("final result: " + i);
        }
        System.out.println("All done! " + i + "(" + failed + "), total: " + (i + failed));
    }

    private void go() throws Exception {
        Pair<Indexable, Persistable> first = db1.first(Milestone.class, IntegerIndex.class);
        int msIndex = ((Milestone)first.hi).index.getValue();

        System.out.println("Starting at " + msIndex);
        

        Pair<Indexable, Persistable> mirrorFirst = db2.first(Milestone.class, IntegerIndex.class);
        int mirrorIndex = ((Milestone)mirrorFirst.hi).index.getValue();
        System.out.println("Mirror lowest: " + mirrorIndex);
        
        while (!db2.exists(Milestone.class, first.low)) {
            first = db1.next(Milestone.class, first.low);
            msIndex = ((Milestone)first.hi).index.getValue();
        }
        
        System.out.println("both nodes start at:  " + msIndex);
        System.out.println(first);

        System.out.println("Statediff count: ");
        System.out.println("m20: " + db1.count(StateDiff.class));
        System.out.println("mirror01: " + db2.count(StateDiff.class));
        
        // Both have the index
        MilestoneViewModel msHash;
        int start = msIndex;
        int failed = 0;
        while ((msHash = MilestoneViewModel.get(db1Tangle, msIndex)) != null) {
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

        StateDiffViewModel stateDiff = StateDiffViewModel.load(db1Tangle, msHash);
        StateDiffViewModel stateDiff2 = StateDiffViewModel.load(db2Tangle, msHash);
        
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




























