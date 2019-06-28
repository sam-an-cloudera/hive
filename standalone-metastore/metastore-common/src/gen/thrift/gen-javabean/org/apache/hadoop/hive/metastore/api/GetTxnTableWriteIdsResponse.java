/**
 * Autogenerated by Thrift Compiler (0.9.3)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.3)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class GetTxnTableWriteIdsResponse implements org.apache.thrift.TBase<GetTxnTableWriteIdsResponse, GetTxnTableWriteIdsResponse._Fields>, java.io.Serializable, Cloneable, Comparable<GetTxnTableWriteIdsResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetTxnTableWriteIdsResponse");

  private static final org.apache.thrift.protocol.TField TABLE_WRITE_IDS_FIELD_DESC = new org.apache.thrift.protocol.TField("tableWriteIds", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new GetTxnTableWriteIdsResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new GetTxnTableWriteIdsResponseTupleSchemeFactory());
  }

  private List<TableWriteId> tableWriteIds; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_WRITE_IDS((short)1, "tableWriteIds");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // TABLE_WRITE_IDS
          return TABLE_WRITE_IDS;
        default:
          return null;
      }
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.TABLE_WRITE_IDS, new org.apache.thrift.meta_data.FieldMetaData("tableWriteIds", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TableWriteId.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetTxnTableWriteIdsResponse.class, metaDataMap);
  }

  public GetTxnTableWriteIdsResponse() {
  }

  public GetTxnTableWriteIdsResponse(
    List<TableWriteId> tableWriteIds)
  {
    this();
    this.tableWriteIds = tableWriteIds;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetTxnTableWriteIdsResponse(GetTxnTableWriteIdsResponse other) {
    if (other.isSetTableWriteIds()) {
      List<TableWriteId> __this__tableWriteIds = new ArrayList<TableWriteId>(other.tableWriteIds.size());
      for (TableWriteId other_element : other.tableWriteIds) {
        __this__tableWriteIds.add(new TableWriteId(other_element));
      }
      this.tableWriteIds = __this__tableWriteIds;
    }
  }

  public GetTxnTableWriteIdsResponse deepCopy() {
    return new GetTxnTableWriteIdsResponse(this);
  }

  @Override
  public void clear() {
    this.tableWriteIds = null;
  }

  public int getTableWriteIdsSize() {
    return (this.tableWriteIds == null) ? 0 : this.tableWriteIds.size();
  }

  public java.util.Iterator<TableWriteId> getTableWriteIdsIterator() {
    return (this.tableWriteIds == null) ? null : this.tableWriteIds.iterator();
  }

  public void addToTableWriteIds(TableWriteId elem) {
    if (this.tableWriteIds == null) {
      this.tableWriteIds = new ArrayList<TableWriteId>();
    }
    this.tableWriteIds.add(elem);
  }

  public List<TableWriteId> getTableWriteIds() {
    return this.tableWriteIds;
  }

  public void setTableWriteIds(List<TableWriteId> tableWriteIds) {
    this.tableWriteIds = tableWriteIds;
  }

  public void unsetTableWriteIds() {
    this.tableWriteIds = null;
  }

  /** Returns true if field tableWriteIds is set (has been assigned a value) and false otherwise */
  public boolean isSetTableWriteIds() {
    return this.tableWriteIds != null;
  }

  public void setTableWriteIdsIsSet(boolean value) {
    if (!value) {
      this.tableWriteIds = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TABLE_WRITE_IDS:
      if (value == null) {
        unsetTableWriteIds();
      } else {
        setTableWriteIds((List<TableWriteId>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_WRITE_IDS:
      return getTableWriteIds();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TABLE_WRITE_IDS:
      return isSetTableWriteIds();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetTxnTableWriteIdsResponse)
      return this.equals((GetTxnTableWriteIdsResponse)that);
    return false;
  }

  public boolean equals(GetTxnTableWriteIdsResponse that) {
    if (that == null)
      return false;

    boolean this_present_tableWriteIds = true && this.isSetTableWriteIds();
    boolean that_present_tableWriteIds = true && that.isSetTableWriteIds();
    if (this_present_tableWriteIds || that_present_tableWriteIds) {
      if (!(this_present_tableWriteIds && that_present_tableWriteIds))
        return false;
      if (!this.tableWriteIds.equals(that.tableWriteIds))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_tableWriteIds = true && (isSetTableWriteIds());
    list.add(present_tableWriteIds);
    if (present_tableWriteIds)
      list.add(tableWriteIds);

    return list.hashCode();
  }

  @Override
  public int compareTo(GetTxnTableWriteIdsResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetTableWriteIds()).compareTo(other.isSetTableWriteIds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableWriteIds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableWriteIds, other.tableWriteIds);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("GetTxnTableWriteIdsResponse(");
    boolean first = true;

    sb.append("tableWriteIds:");
    if (this.tableWriteIds == null) {
      sb.append("null");
    } else {
      sb.append(this.tableWriteIds);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetTableWriteIds()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'tableWriteIds' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetTxnTableWriteIdsResponseStandardSchemeFactory implements SchemeFactory {
    public GetTxnTableWriteIdsResponseStandardScheme getScheme() {
      return new GetTxnTableWriteIdsResponseStandardScheme();
    }
  }

  private static class GetTxnTableWriteIdsResponseStandardScheme extends StandardScheme<GetTxnTableWriteIdsResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetTxnTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // TABLE_WRITE_IDS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list674 = iprot.readListBegin();
                struct.tableWriteIds = new ArrayList<TableWriteId>(_list674.size);
                TableWriteId _elem675;
                for (int _i676 = 0; _i676 < _list674.size; ++_i676)
                {
                  _elem675 = new TableWriteId();
                  _elem675.read(iprot);
                  struct.tableWriteIds.add(_elem675);
                }
                iprot.readListEnd();
              }
              struct.setTableWriteIdsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetTxnTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableWriteIds != null) {
        oprot.writeFieldBegin(TABLE_WRITE_IDS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.tableWriteIds.size()));
          for (TableWriteId _iter677 : struct.tableWriteIds)
          {
            _iter677.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetTxnTableWriteIdsResponseTupleSchemeFactory implements SchemeFactory {
    public GetTxnTableWriteIdsResponseTupleScheme getScheme() {
      return new GetTxnTableWriteIdsResponseTupleScheme();
    }
  }

  private static class GetTxnTableWriteIdsResponseTupleScheme extends TupleScheme<GetTxnTableWriteIdsResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetTxnTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      {
        oprot.writeI32(struct.tableWriteIds.size());
        for (TableWriteId _iter678 : struct.tableWriteIds)
        {
          _iter678.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetTxnTableWriteIdsResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list679 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.tableWriteIds = new ArrayList<TableWriteId>(_list679.size);
        TableWriteId _elem680;
        for (int _i681 = 0; _i681 < _list679.size; ++_i681)
        {
          _elem680 = new TableWriteId();
          _elem680.read(iprot);
          struct.tableWriteIds.add(_elem680);
        }
      }
      struct.setTableWriteIdsIsSet(true);
    }
  }

}

