package com.xxo.utils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class UnZip {
	public static void visitTARGZ(File targzFile, FSDataOutputStream out) {
		FileInputStream fileIn = null;
		BufferedInputStream bufIn = null;
		GZIPInputStream gzipIn = null;
		TarArchiveInputStream taris = null;
		if (!targzFile.exists()) {
			return;
		}
		try {
			fileIn = new FileInputStream(targzFile);
			bufIn = new BufferedInputStream(fileIn);

			gzipIn = new GZIPInputStream(bufIn); // first unzip the input file
			taris = new TarArchiveInputStream(gzipIn);
			TarArchiveEntry entry = null;
			while ((entry = taris.getNextTarEntry()) != null) {
				if (entry.isDirectory())
					continue;
				byte buffer[] = new byte[10240];
				int bytesRead = 0;
				while ((bytesRead = taris.read(buffer)) > 0) {
					out.write(buffer, 0, bytesRead);
				}
				out.flush();
			}
		} catch (IOException e) {
			System.out.println("IOException: ");
			e.printStackTrace();
		} finally {
			try {
				taris.close();
				gzipIn.close();
				bufIn.close();
				fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public static void nioReadFile(File flatFile, FSDataOutputStream out) {
		FileInputStream fin = null;
		ByteBuffer buffer = null;
		FileChannel inch = null;
		byte[] bs = new byte[512];
		try {
			fin = new FileInputStream(flatFile);
			buffer = ByteBuffer.allocate(512);
			inch = fin.getChannel();

			int readsize = 0;

			while ((readsize = inch.read(buffer)) != -1) {
				readsize = buffer.position();
				buffer.rewind();
				buffer.get(bs);
				buffer.flip();
				buffer.clear();
				if(readsize<=0)
				{
					System.out.println("readsize: "+readsize);
					return ;
				}
				out.write(bs, 0, readsize);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fin.close();
				inch.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void readFileByIO(String fileName) {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(fileName);
			byte[] buffer = new byte[1024];
			int len = 0;
			while ((len = fis.read(buffer)) != -1) {
				System.out.println(len);
				System.out.write(buffer, 0, len);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		UnZip.nioReadFile(new File("hs_err_pid6392.log"), null);
		// readFileByIO("hs_err_pid6392.log");
		// readByNIO("hs_err_pid6392.log");
	}

}
