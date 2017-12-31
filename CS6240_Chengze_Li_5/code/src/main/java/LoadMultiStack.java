import java.awt.image.BufferedImage;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoadMultiStack
{
	private static final Logger logger = LogManager.getLogger(LoadMultiStack.class);
	private static final String bucketName = "chengze-assignment5";

	static AmazonS3 s3Client = new AmazonS3Client();

	public static byte[][] process(final String args[]) {

		try {
			final String key = args[0];
			final InputStream imageFile = checkFileAws(bucketName, key);
			final int xDim = Integer.parseInt(args[1]);
			final int yDim = Integer.parseInt(args[2]);
			final ImageReader reader = buildAwsReader(imageFile);
			final int zDim = reader.getNumImages(true);
			final byte imageBytes[][] = new byte[1][xDim * yDim * zDim];

			for (int ix = 0; ix < zDim; ix++) {
				final BufferedImage image = reader.read(ix);
				final DataBuffer dataBuffer = image.getRaster().getDataBuffer();
				final byte layerBytes[] = ((DataBufferByte)dataBuffer).getData();
				System.arraycopy(layerBytes, 0, imageBytes[0], ix * xDim * yDim, xDim * yDim);
			}

//	        for (int iz = 0 ; iz < zDim ; iz++) {
//		        for (int iy = 0 ; iy < yDim ; iy++) {
//			        for (int ix = 0 ; ix < xDim ; ix++) {
//			        	System.out.println(imageBytes[iz * yDim * xDim + iy * xDim + ix]);
//			        }
//		        }
//	        }
			return imageBytes;
		} catch (final Exception e) {
			logger.error("", e);
			return new byte[0][0];
		}
	}

	private static File checkFile(final String fileName) throws Exception {
		final File imageFile = new File(fileName);
		if (!imageFile.exists() || imageFile.isDirectory()) {
			throw new Exception ("Image file does not exist: " + fileName);
		}
		return imageFile;
	}

	private static InputStream checkFileAws(final String bucketName, final String key) throws Exception {
		S3Object imageFile = s3Client.getObject(
				new GetObjectRequest(bucketName, key));
		InputStream imageFileData = imageFile.getObjectContent();
		return imageFileData;
	}

	private static ImageReader buildReader(final File imageFile) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageFile);
		if (imgInStream == null || imgInStream.length() == 0){
			throw new Exception("Data load error - No input stream.");
		}
		Iterator<ImageReader> iter = ImageIO.getImageReaders(imgInStream);
		if (iter == null || !iter.hasNext()) {
			throw new Exception("Data load error - Image file format not supported by ImageIO.");
		}
		final ImageReader reader = iter.next();
		iter = null;
		reader.setInput(imgInStream);
		return reader;
	}

	private static ImageReader buildAwsReader(final InputStream imageFile) throws Exception {
		final ImageInputStream imgInStream = ImageIO.createImageInputStream(imageFile);
		if (imgInStream == null || imgInStream.length() == 0){
			throw new Exception("Data load error - No input stream.");
		}
		Iterator<ImageReader> iter = ImageIO.getImageReaders(imgInStream);
		if (iter == null || !iter.hasNext()) {
			throw new Exception("Data load error - Image file format not supported by ImageIO.");
		}
		final ImageReader reader = iter.next();

		reader.setInput(imgInStream);
		return reader;
	}
}