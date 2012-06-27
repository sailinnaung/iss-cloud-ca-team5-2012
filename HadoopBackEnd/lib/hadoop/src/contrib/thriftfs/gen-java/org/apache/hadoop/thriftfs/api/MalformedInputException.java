/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.hadoop.thriftfs.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import com.facebook.thrift.*;

import com.facebook.thrift.protocol.*;
import com.facebook.thrift.transport.*;

public class MalformedInputException extends Exception implements TBase, java.io.Serializable {
  public String message;

  public final Isset __isset = new Isset();
  public static final class Isset implements java.io.Serializable {
    public boolean message = false;
  }

  public MalformedInputException() {
  }

  public MalformedInputException(
    String message)
  {
    this();
    this.message = message;
    this.__isset.message = true;
  }

  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof MalformedInputException)
      return this.equals((MalformedInputException)that);
    return false;
  }

  public boolean equals(MalformedInputException that) {
    if (that == null)
      return false;

    boolean this_present_message = true && (this.message != null);
    boolean that_present_message = true && (that.message != null);
    if (this_present_message || that_present_message) {
      if (!(this_present_message && that_present_message))
        return false;
      if (!this.message.equals(that.message))
        return false;
    }

    return true;
  }

  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case -1:
          if (field.type == TType.STRING) {
            this.message = iprot.readString();
            this.__isset.message = true;
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();
  }

  public void write(TProtocol oprot) throws TException {
    TStruct struct = new TStruct("MalformedInputException");
    oprot.writeStructBegin(struct);
    TField field = new TField();
    if (this.message != null) {
      field.name = "message";
      field.type = TType.STRING;
      field.id = -1;
      oprot.writeFieldBegin(field);
      oprot.writeString(this.message);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("MalformedInputException(");
    sb.append("message:");
    sb.append(this.message);
    sb.append(")");
    return sb.toString();
  }

}

