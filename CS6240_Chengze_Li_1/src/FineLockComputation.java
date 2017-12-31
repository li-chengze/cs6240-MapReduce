import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * this class is used to create a thread object which only add lock on "part of " shared data structure,
 * instead of whole structure
 */

public class FineLockComputation extends Thread{

    private ConcurrentHashMap<String, int[]> map;
    private List<String> content;

    public FineLockComputation(List<String> content, ConcurrentHashMap<String, int[]> map){
        this.map = map;
        this.content = content;
    }

    public void run(){
        int length = content.size();

        for(int i = 0; i < length; i++) {
            String[] data = content.get(i).split(",");
            fibonacci(17);
            if (!data[2].equals("TMAX")){
                continue;
            }

            if (!map.containsKey(data[0])) {
                map.put(data[0], new int[2]);
            }

            map.get(data[0])[0] += Integer.parseInt(data[3]);
            map.get(data[0])[1] ++;

        }
    }

    public double getAverageMaxByStation(String stationName) {
        if (map.get(stationName) == null) {
            return -999;  // if the result of this function is -999.0, means we don't get the max temprature of this station
        }

        int total = map.get(stationName)[0];
        int number = map.get(stationName)[1];

        return total / number;
    }

    private long fibonacci(int n) {
        if (n <= 1) return n;
        else return fibonacci(n-1) + fibonacci(n-2);
    }
}
