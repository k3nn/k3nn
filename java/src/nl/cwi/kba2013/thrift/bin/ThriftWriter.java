package nl.cwi.kba2013.thrift.bin;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TIOStreamTransport;


import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * Simple class that makes it easy to write Thrift objects to disk.

 */
public class ThriftWriter {
  /** File to write to. */
  protected final File file;
  
  /** For writing to the file. */
  private BufferedOutputStream bufferedOut;

  /** For binary serialization of objects. */
  private TBinaryProtocol binaryOut;
  
  /**
    * Constructor.
    */
  public ThriftWriter(File file) {
    this.file = file;
  }
  
  /**
    * Open the file for writing.
    */
  public void open() throws FileNotFoundException {
    bufferedOut = new BufferedOutputStream(new FileOutputStream(file), 2048);
    binaryOut = new TBinaryProtocol(new TIOStreamTransport(bufferedOut));
  }
  
  /**
    * Write the object to disk.
    */
  public void write(TBase t) throws IOException {
    try {
      t.write(binaryOut);
      bufferedOut.flush();
    } catch (TException e) {
      throw new IOException(e);
    }
  }
  
  /**
    * Close the file stream.
    */
  public void close() throws IOException {
    bufferedOut.close();
  }
}

