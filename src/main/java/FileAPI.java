import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.List;

public class FileAPI {

    public void createFile(String fileName) throws Exception {
        Configuration conf = new Configuration();

        // Path of the custom conf file
        String pathToConfFile = "C:/hadoop-3.3.6/etc/hadoop/core-site.xml";
        conf.addResource(new Path(pathToConfFile)); // add this so the settings are updated

        //System.out.println(conf.getRaw("fs.default.name"));

        FileSystem fs = FileSystem.get(conf);
        //
        FSDataOutputStream out = fs.create(new Path(fileName));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        //
        for (int i = 0; i < 25; i++) {
            bw.write("File Record " + i);
            bw.newLine();
        }
        bw.close();
    }

    public void getMetadata(String fileName) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
        Path path = new Path(fileName);
        FileStatus fileStatus = fs.getFileStatus(path);

        System.out.println("++++++++++++++++++++");
        System.out.println("Directory flag: " + fileStatus.isDirectory());
        System.out.println("Owner of the file: " + fileStatus.getOwner());
        System.out.println("File Block Size: " + fileStatus.getBlockSize());
        System.out.println("File Path Name: " + fileStatus.getPath());
        System.out.println("File Permission: " + fileStatus.getPermission());
        System.out.println("File Modification Time: " + fileStatus.getModificationTime());

        // Option 1 - Get the block locations
        BlockLocation[] locations = fs.getFileBlockLocations(path, 0, 1000);
        for (BlockLocation bl : locations) {
            System.out.println("Block Offset: " +bl.getOffset());
            System.out.println("Block Length: " +bl.getLength());
            System.out.println("Block Host: " +bl.getHosts()[0]);
            System.out.println("Block Location : " +bl.toString());
        }
        // Option 2 - Get block locations
        System.out.println("++++++++++++++++++++++++++++");
        HdfsDataInputStream hdis = (HdfsDataInputStream) fs.open(path);
        List<LocatedBlock> lblocks = hdis.getAllBlocks();
        System.out.println("The number of blocks in the file: " + lblocks.size());
        for (LocatedBlock lblk : lblocks) {
            System.out.println("The Start Offset: " + lblk.getStartOffset());
            System.out.println("The Block Size: " + lblk.getBlockSize());
            System.out.println("The Block Type: " + lblk.getBlockType());
            System.out.println("The Block Token: " + lblk.getBlockToken());
            System.out.println("The Datanode Name: " + lblk.getLocations()[0].getHostName());
            ExtendedBlock eb = lblk.getBlock();
            System.out.println("The Block Name: " + eb.getBlockName());
            System.out.println("The Block Pool ID: " + eb.getBlockPoolId());
            System.out.println("The Block ID: " + eb.getBlockId());
            Block lb = eb.getLocalBlock();
            System.out.println("Number of Bytes: " + lb.getNumBytes());
        }
    }

    public void remoteReadFile(String fileName) throws Exception {
        Configuration conf = new Configuration();
        //
        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
        Path path = new Path(fileName);
        FSDataInputStream in = fs.open(path);
        in.seek(0);     // Position back to start of the file
        InputStreamReader reader = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(reader);
        String line = null;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("here we go: " + args[0]);
        new FileAPI().createFile(args[0]);
        new FileAPI().remoteReadFile(args[0]);
        new FileAPI().getMetadata(args[0]);
    }
}
