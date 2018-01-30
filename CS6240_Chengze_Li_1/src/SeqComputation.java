import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this class doesn't use multi threads , only use one thread to compute result
 */
public class SeqComputation {

    Map<String, int[]> map = new HashMap<>();

    public void initialize(List<String> records){
        int length = records.size();

        for(int i = 0; i < length; i++){
            String[] data = records.get(i).split(",");
            //fibonacci(17);
            if (data[2].equals("TMAX")){
                if (map.get(data[0]) == null) {
                    map.put(data[0], new int[2]);
                }
                map.get(data[0])[0] += Integer.parseInt(data[3]);
                map.get(data[0])[1] ++;
            }
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

    private static long fibonacci(int n) {
        if (n <= 1) return n;
        else return fibonacci(n-1) + fibonacci(n-2);
    }
}
