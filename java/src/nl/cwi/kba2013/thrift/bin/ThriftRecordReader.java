/*******************************************************************************
 * Copyright 2012 Edgar Meij
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package nl.cwi.kba2013.thrift.bin;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * RecordReader that emits filename, StreamItemWritable pairs. 
 * 
 * @author emeij
 *
 */
public class ThriftRecordReader extends RecordReader<Text, StreamItemWritable> {
	private static final Logger LOG = Logger.getLogger(ThriftRecordReader.class);

  private DataInputStream in;
  private TProtocol tp;
  private long start;
  private long length;
  private long position;
  private Text key = new Text();
  private StreamItemWritable value = new StreamItemWritable();
  private FileSplit fileSplit;
  private Configuration conf;
  private CompressionCodecFactory compressionCodecs;
  private CompressionCodec codec;
  private Decompressor decompressor;
  private Seekable filePosition;

  public ThriftRecordReader(FileSplit fileSplit, Configuration conf)
      throws IOException {
    this.fileSplit = fileSplit;
    this.conf = conf;
  }

  @Override
  public void close() throws IOException {
    if (in != null)
      in.close();
  }

  /** Returns our progress within the split, as a float between 0 and 1. */
  @Override
  public float getProgress() {

    if (length == 0)
      return 0.0f;

    return Math.min(1.0f, position / (float) length);

  }
  
  /** Boilerplate initialization code for file input streams. */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    conf = context.getConfiguration();
    fileSplit = (FileSplit) split;
    start = fileSplit.getStart();
    length = fileSplit.getLength();
    position = start;

    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    FSDataInputStream fileIn = fs.open(path);

	compressionCodecs = new CompressionCodecFactory(conf);
	codec = compressionCodecs.getCodec(path);

	if (isCompressedInput()) {
		decompressor = CodecPool.getDecompressor(codec);
		in = new DataInputStream(codec.createInputStream(fileIn, decompressor));
		filePosition = fileIn;
		//LOG.info("Successfully initialized input stream for compressed data.");
	} else {
		fileIn.seek(start);
		in = fileIn;
		filePosition = fileIn;
	}
	
    tp = new TBinaryProtocol.Factory().getProtocol(new TIOStreamTransport(in));
  }

//  private long getFilePosition() throws IOException {
//	  long retVal;
//	  if(isCompressedInput()&& null != filePosition) {
//		  retVal = filePosition.getPos();
//	  } else {
//		  retVal = position;
//	  }
//	  return retVal;
//  }
  public boolean isCompressedInput() {
	  return (codec != null);
  }

  @Override
  /**
   * parse the next key value, update position and return true
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {

    // key
    // key.set(fileSplit.getPath().getName().toString());
    key.set(fileSplit.getPath().toString());

//    // value
//    if (in.available() > 0) { // && position - start < length) {
//
//      try {
//        value.read(tp);
//        position = length - in.available() - start;
//      } catch (TException e) {
//        e.printStackTrace();
//        throw new IOException(e);
//      }
//
//    } else {
//      return false;
//    }

  // value
    try {
      value.read(tp);
      //LOG.info(value);
    } catch (TException e) {
      return false;
    }

    return true;

  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public StreamItemWritable getCurrentValue() throws IOException,
      InterruptedException {
    return value;
  }
}


//package nl.cwi.kba2013.thrift.bin;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.protocol.TProtocol;
//import org.apache.thrift.transport.TIOStreamTransport;
//
///**
// * RecordReader that emits filename, StreamItemWritable pairs. 
// * 
// * @author emeij
// *
// */
//public class ThriftRecordReader extends RecordReader<Text, StreamItemWritable> {
//
//  private FSDataInputStream in;
//  private TProtocol tp;
//  private long start;
//  private long length;
//  private long position;
//  private Text key = new Text();
//  private StreamItemWritable value = new StreamItemWritable();
//  private FileSplit fileSplit;
//  private Configuration conf;
//
//  public ThriftRecordReader(FileSplit fileSplit, Configuration conf)
//      throws IOException {
//    this.fileSplit = fileSplit;
//    this.conf = conf;
//  }
//
//  @Override
//  public void close() throws IOException {
//    if (in != null)
//      in.close();
//  }
//
//  /** Returns our progress within the split, as a float between 0 and 1. */
//  @Override
//  public float getProgress() {
//
//    if (length == 0)
//      return 0.0f;
//
//    return Math.min(1.0f, position / (float) length);
//
//  }
//
//  /** Boilerplate initialization code for file input streams. */
//  @Override
//  public void initialize(InputSplit split, TaskAttemptContext context)
//      throws IOException, InterruptedException {
//
//    conf = context.getConfiguration();
//    fileSplit = (FileSplit) split;
//    start = fileSplit.getStart();
//    length = fileSplit.getLength();
//    position = 0;
//
//    Path path = fileSplit.getPath();
//    FileSystem fs = path.getFileSystem(conf);
//    in = fs.open(path);
//
//    tp = new TBinaryProtocol.Factory().getProtocol(new TIOStreamTransport(in));
//
//  }
//
//  @Override
//  /**
//   * parse the next key value, update position and return true
//   */
//  public boolean nextKeyValue() throws IOException, InterruptedException {
//
//    // key
//    // key.set(fileSplit.getPath().getName().toString());
//    key.set(fileSplit.getPath().toString());
//
//    // value
//    if (in.available() > 0) { // && position - start < length) {
//
//      try {
//        value.read(tp);
//        position = length - in.available() - start;
//      } catch (Exception e) {
//        e.printStackTrace();
//        throw new IOException(e);
//      }
//
//    } else {
//      return false;
//    }
//
//    return true;
//
//  }
//
//  @Override
//  public Text getCurrentKey() throws IOException, InterruptedException {
//    return key;
//  }
//
//  @Override
//  public StreamItemWritable getCurrentValue() throws IOException,
//      InterruptedException {
//    return value;
//  }
//}
//
