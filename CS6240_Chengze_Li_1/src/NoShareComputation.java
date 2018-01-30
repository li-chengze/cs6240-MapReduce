import java.util.List;
import java.util.Map;

/**
 * this class is used to create thread object which hold their own data structure, they only update
 * their own data, and don't talk with each other
 */
public class NoShareComputation extends Thread {

    private List<String> content;
    private Map<String, int[]> map;

    public NoShareComputation(List<String> content, Map<String, int[]> map) {
        this.content = content;
        this.map = map;
    }

    public void run() {
        int length = content.size();
        for (int i = 0; i < length; i++) {
            String[] data = content.get(i).split(",");
            //fibonacci(17);
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

    private long fibonacci(int n) {
        if (n <= 1) return n;
        else return fibonacci(n-1) + fibonacci(n-2);
    }

}
