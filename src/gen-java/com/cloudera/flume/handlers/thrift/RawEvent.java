/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.cloudera.flume.handlers.thrift;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class RawEvent implements TBase<RawEvent._Fields>, java.io.Serializable, Cloneable, Comparable<RawEvent> {
  private static final TStruct STRUCT_DESC = new TStruct("RawEvent");

  private static final TField RAW_FIELD_DESC = new TField("raw", TType.STRING, (short)1);

  public byte[] raw;

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements TFieldIdEnum {
    RAW((short)1, "raw");

    private static final Map<Integer, _Fields> byId = new HashMap<Integer, _Fields>();
    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byId.put((int)field._thriftId, field);
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      return byId.get(fieldId);
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments

  public static final Map<_Fields, FieldMetaData> metaDataMap = Collections.unmodifiableMap(new EnumMap<_Fields, FieldMetaData>(_Fields.class) {{
    put(_Fields.RAW, new FieldMetaData("raw", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
  }});

  static {
    FieldMetaData.addStructMetaDataMap(RawEvent.class, metaDataMap);
  }

  public RawEvent() {
  }

  public RawEvent(
    byte[] raw)
  {
    this();
    this.raw = raw;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RawEvent(RawEvent other) {
    if (other.isSetRaw()) {
      this.raw = new byte[other.raw.length];
      System.arraycopy(other.raw, 0, raw, 0, other.raw.length);
    }
  }

  public RawEvent deepCopy() {
    return new RawEvent(this);
  }

  @Deprecated
  public RawEvent clone() {
    return new RawEvent(this);
  }

  public byte[] getRaw() {
    return this.raw;
  }

  public RawEvent setRaw(byte[] raw) {
    this.raw = raw;
    return this;
  }

  public void unsetRaw() {
    this.raw = null;
  }

  /** Returns true if field raw is set (has been asigned a value) and false otherwise */
  public boolean isSetRaw() {
    return this.raw != null;
  }

  public void setRawIsSet(boolean value) {
    if (!value) {
      this.raw = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case RAW:
      if (value == null) {
        unsetRaw();
      } else {
        setRaw((byte[])value);
      }
      break;

    }
  }

  public void setFieldValue(int fieldID, Object value) {
    setFieldValue(_Fields.findByThriftIdOrThrow(fieldID), value);
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case RAW:
      return getRaw();

    }
    throw new IllegalStateException();
  }

  public Object getFieldValue(int fieldId) {
    return getFieldValue(_Fields.findByThriftIdOrThrow(fieldId));
  }

  /** Returns true if field corresponding to fieldID is set (has been asigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    switch (field) {
    case RAW:
      return isSetRaw();
    }
    throw new IllegalStateException();
  }

  public boolean isSet(int fieldID) {
    return isSet(_Fields.findByThriftIdOrThrow(fieldID));
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RawEvent)
      return this.equals((RawEvent)that);
    return false;
  }

  public boolean equals(RawEvent that) {
    if (that == null)
      return false;

    boolean this_present_raw = true && this.isSetRaw();
    boolean that_present_raw = true && that.isSetRaw();
    if (this_present_raw || that_present_raw) {
      if (!(this_present_raw && that_present_raw))
        return false;
      if (!java.util.Arrays.equals(this.raw, that.raw))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(RawEvent other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    RawEvent typedOther = (RawEvent)other;

    lastComparison = Boolean.valueOf(isSetRaw()).compareTo(isSetRaw());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(raw, typedOther.raw);
    if (lastComparison != 0) {
      return lastComparison;
    }
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
      _Fields fieldId = _Fields.findByThriftId(field.id);
      if (fieldId == null) {
        TProtocolUtil.skip(iprot, field.type);
      } else {
        switch (fieldId) {
          case RAW:
            if (field.type == TType.STRING) {
              this.raw = iprot.readBinary();
            } else { 
              TProtocolUtil.skip(iprot, field.type);
            }
            break;
        }
        iprot.readFieldEnd();
      }
    }
    iprot.readStructEnd();

    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    if (this.raw != null) {
      oprot.writeFieldBegin(RAW_FIELD_DESC);
      oprot.writeBinary(this.raw);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("RawEvent(");
    boolean first = true;

    sb.append("raw:");
    if (this.raw == null) {
      sb.append("null");
    } else {
        int __raw_size = Math.min(this.raw.length, 128);
        for (int i = 0; i < __raw_size; i++) {
          if (i != 0) sb.append(" ");
          sb.append(Integer.toHexString(this.raw[i]).length() > 1 ? Integer.toHexString(this.raw[i]).substring(Integer.toHexString(this.raw[i]).length() - 2).toUpperCase() : "0" + Integer.toHexString(this.raw[i]).toUpperCase());
        }
        if (this.raw.length > 128) sb.append(" ...");
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
  }

}
