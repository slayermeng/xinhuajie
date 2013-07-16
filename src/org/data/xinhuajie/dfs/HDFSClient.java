package org.data.xinhuajie.dfs;

import java.net.URI;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.io.UTF8;

public class HDFSClient {
	private static final Configuration conf = new Configuration();

	private static FileSystem fs = null;

	private String uri = "hdfs://10.101.22.51:9000";

	public HDFSClient() throws IOException {
		fs = FileSystem.get(URI.create(uri), conf);
	}

	public boolean isExist(String path) throws IOException {
		return fs.exists(new Path(path));
	}

	public boolean isDirectory(String path) throws IOException {
		return fs.getFileStatus(new Path(path)).isDir();
	}

	public boolean isFile(String path) throws IOException {
		return fs.isFile(new Path(path));
	}

	public boolean mkdir(String path) throws IOException {
		return fs.mkdirs(new Path(path));
	}
	
	public boolean delete(String path) throws IOException{
		return fs.delete(new Path(path), true);
	}

	public void stat() throws IOException {
		FileSystem.printStatistics();
	}

	public void readFSImage() throws IOException {
		DataInputStream in = new DataInputStream(new BufferedInputStream(
				new FileInputStream("d://fsimage")));
		int imgVersion = in.readInt();
		int namespaceID = in.readInt();
		long numFiles;
		if (imgVersion <= -16) {
			numFiles = in.readLong();
		} else {
			numFiles = in.readInt();
		}
		if (imgVersion <= -12) {
			long genstamp = in.readLong();
		}
		UTF8 U_STR = new UTF8();
		for (long i = 0; i < numFiles; i++) {
			long modificationTime = 0;
	        long atime = 0;
	        long blockSize = 0;
			U_STR.readFields(in);
			String path = U_STR.toString();
			System.out.println(path);
			in.readShort();
			in.readLong();
			if (imgVersion <= -17) {
		          atime = in.readLong();
		        }
		        if (imgVersion <= -8) {
		          blockSize = in.readLong();
		        }
		        int numBlocks = in.readInt();
		        Block blocks[] = null;

		        // for older versions, a blocklist of size 0
		        // indicates a directory.
		        if ((-9 <= imgVersion && numBlocks > 0) ||
		            (imgVersion < -9 && numBlocks >= 0)) {
		          blocks = new Block[numBlocks];
		          for (int j = 0; j < numBlocks; j++) {
		            blocks[j] = new Block();
		            if (-14 < imgVersion) {
		              blocks[j].set(in.readLong(), in.readLong(), 
		                            Block.GRANDFATHER_GENERATION_STAMP);
		            } else {
		              blocks[j].readFields(in);
		            }
		          }
		        }
		        // Older versions of HDFS does not store the block size in inode.
		        // If the file has more than one block, use the size of the 
		        // first block as the blocksize. Otherwise use the default block size.
		        //
		        if (-8 <= imgVersion && blockSize == 0) {
		          if (numBlocks > 1) {
		            blockSize = blocks[0].getNumBytes();
		          } else {
		            long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
		            
		          }
		        }
		        
		        // get quota only when the node is a directory
		        long nsQuota = -1L;
		        if (imgVersion <= -16 && blocks == null) {
		          nsQuota = in.readLong();
		        }
		        long dsQuota = -1L;
		        if (imgVersion <= -18 && blocks == null) {
		          dsQuota = in.readLong();
		        }
		}
		System.out.println(imgVersion);
		System.out.println(namespaceID);
		System.out.println(numFiles);
	}

	public static void main(String[] args) throws Exception {
		HDFSClient client = new HDFSClient();
		System.out.println(client.isExist("/out/201302"));
	}
}
