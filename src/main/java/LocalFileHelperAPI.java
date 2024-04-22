import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class LocalFileHelperAPI {

    /***
     * Delete a directory if it exists.
     * @param path Path in string form of the directory
     */
    public static void DeleteDirectory(String path) {
        try {
            File file = new File(path);

            if (file.exists()) {
                FileUtils.cleanDirectory(file);
                FileUtils.forceDelete(file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
