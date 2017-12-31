import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    static List<String> content;
    public static void main(String[] args) {
        FileLoader fl = new FileLoader();

        // specify the relative path of input file
        content = fl.getContent("src/1912.csv");

        // the number of lines inside input file
        int length = content.size();

        // seq computation
        double result0 = seqComputation("USC00144932");
        System.out.println("sequential computation result: " + result0);

        // no lock computation
        double result1 = noLockComputation("USC00144932", length);
        System.out.println("no lock computation result: " + result1);


        // coarse lock computation
        double result2 = coarseLockComputation("USC00144932", length);
        System.out.println("coarse lock computation result: " + result2);


        // fine lock computation
        double result3 = fineLockComputation("USC00144932", length);
        System.out.println("fine lock computation result: " + result3);


        // no share computation
        double result4 = noShareComputation("USC00144932", length);
        System.out.println("no share computation result: " + result4);
    }

    private static double seqComputation(String stationName) {
        long startTime = System.currentTimeMillis();

        // sequential computation
        SeqComputation seq = new SeqComputation();
        seq.initialize(content);
        double result = seq.getAverageMaxByStation(stationName);

        long stopTime = System.currentTimeMillis();

        System.out.println("sequential computation consume time: " + (stopTime - startTime));

        return result;
    }

    private static double noLockComputation(String stationName, int length) {
        long startTime = System.currentTimeMillis();

        //shared data
        Map<String, int[]> share = new HashMap<>();

        // create thread object
        NoLockComputation noLock1 = new NoLockComputation(content.subList(0, length / 4 - 1), share);
        NoLockComputation noLock2 = new NoLockComputation(content.subList(length / 4, length / 2 - 1), share);
        NoLockComputation noLock3 = new NoLockComputation(content.subList(length / 2, 3 * length / 4 - 1), share);
        NoLockComputation noLock4 = new NoLockComputation(content.subList(3 * length / 4, length - 1), share);

        // start every thread
        noLock1.start();
        noLock2.start();
        noLock3.start();
        noLock4.start();

        // wait for every thread complete
        try {
            noLock1.join();
            noLock2.join();
            noLock3.join();
            noLock4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long stopTime = System.currentTimeMillis();

        System.out.println("no lock computation consume time: " + (stopTime - startTime));

        return noLock1.getAverageMaxByStation(stationName);
    }

    private static double coarseLockComputation(String stationName, int length) {
        long startTime = System.currentTimeMillis();

        // shared data structure
        Map<String, int[]> share = new HashMap<>();

        // create thread objects
        CoarseLockComputation coarseLock1 = new CoarseLockComputation(content.subList(0, length / 4), share, "thread1");
        CoarseLockComputation coarseLock2 = new CoarseLockComputation(content.subList(length / 4, length / 2), share, "thread2");
        CoarseLockComputation coarseLock3 = new CoarseLockComputation(content.subList(length / 2, 3 * length / 4), share, "thread3");
        CoarseLockComputation coarseLock4 = new CoarseLockComputation(content.subList(3 * length / 4, length), share, "thread4");

        //start every thread
        coarseLock1.start();
        coarseLock2.start();
        coarseLock3.start();
        coarseLock4.start();

        // wait for every thread complete
        try {
            coarseLock1.join();
            coarseLock2.join();
            coarseLock3.join();
            coarseLock4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long stopTime = System.currentTimeMillis();
        System.out.println("coarse lock computation consume time: " + (stopTime - startTime));

        return coarseLock1.getAverageMaxByStation(stationName);
    }

    private static double fineLockComputation(String stationName, int length) {

        long startTime = System.currentTimeMillis();

        // use a build-in structure to apply lock on "part of" structure
        ConcurrentHashMap<String, int[]> share = new ConcurrentHashMap<>();

        // create threads objects
        FineLockComputation fineLock1 = new FineLockComputation(content.subList(0, length / 4), share);
        FineLockComputation fineLock2 = new FineLockComputation(content.subList(length / 4, length / 2), share);
        FineLockComputation fineLock3 = new FineLockComputation(content.subList(length / 2, 3 * length / 4), share);
        FineLockComputation fineLock4 = new FineLockComputation(content.subList(3 * length / 4, length), share);

        // start every thread
        fineLock1.start();
        fineLock2.start();
        fineLock3.start();
        fineLock4.start();

        // wait for every thread completes
        try {
            fineLock1.join();
            fineLock2.join();
            fineLock3.join();
            fineLock4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long stopTime = System.currentTimeMillis();

        System.out.println("fine lock computation consume time: " + (stopTime - startTime));
        return fineLock1.getAverageMaxByStation(stationName);
    }

    private static double noShareComputation(String stationName, int length) {

        long startTime = System.currentTimeMillis();

        // each thread has their own data structure
        Map<String, int[]> map1 = new HashMap<>();
        Map<String, int[]> map2 = new HashMap<>();
        Map<String, int[]> map3 = new HashMap<>();
        Map<String, int[]> map4 = new HashMap<>();

        // this is used to collect the result of all maps above
        HashMap<String, int[]> finalMap;

        NoShareComputation nsLock1 = new NoShareComputation(content.subList(0, length / 4), map1);
        NoShareComputation nsLock2 = new NoShareComputation(content.subList(length / 4, length / 2), map2);
        NoShareComputation nsLock3 = new NoShareComputation(content.subList(length / 2, 3 * length / 4), map3);
        NoShareComputation nsLock4 = new NoShareComputation(content.subList(3 * length / 4, length), map4);

        nsLock1.start();
        nsLock2.start();
        nsLock3.start();
        nsLock4.start();

        try {
            nsLock1.join();
            nsLock2.join();
            nsLock3.join();
            nsLock4.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // collect the data inside threads' map
        finalMap = mergeMap(map1, map2);
        finalMap = mergeMap(finalMap, map3);
        finalMap = mergeMap(finalMap, map4);

        int[] stationData = finalMap.get(stationName);

        long stopTime = System.currentTimeMillis();

        System.out.println("no share computation consume time: " + (stopTime - startTime));

        return (double)(stationData[0] / stationData[1]);
    }

    private static HashMap<String, int[]> mergeMap(Map<String, int[]> map1, Map<String, int[]> map2) {

        map1.forEach(
                (k, v) -> map2.merge(
                        k,
                        v,
                        (v1, v2) -> new int[]{
                                v1[0] + v2[0],
                                v1[1] + v2[1]
                        }));

        return new HashMap<>(map2);
    }
}
