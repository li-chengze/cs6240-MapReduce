import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * this class is used to import file and store the content into a string list
 */
public class FileLoader {
    public List<String> getContent(String filePath){
        List<String> result = new ArrayList<>();

        try{
            File f = new File(filePath);
            BufferedReader br = new BufferedReader(new FileReader(f));
            String readLine = "";
            while((readLine = br.readLine()) != null) {
                result.add(readLine);
            }
        } catch(IOException e){
            e.printStackTrace();
        }

        return result;
    }
}
