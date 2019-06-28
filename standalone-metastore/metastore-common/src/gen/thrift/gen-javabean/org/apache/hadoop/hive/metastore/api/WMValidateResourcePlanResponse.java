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
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class WMValidateResourcePlanResponse implements org.apache.thrift.TBase<WMValidateResourcePlanResponse, WMValidateResourcePlanResponse._Fields>, java.io.Serializable, Cloneable, Comparable<WMValidateResourcePlanResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WMValidateResourcePlanResponse");

  private static final org.apache.thrift.protocol.TField ERRORS_FIELD_DESC = new org.apache.thrift.protocol.TField("errors", org.apache.thrift.protocol.TType.LIST, (short)1);
  private static final org.apache.thrift.protocol.TField WARNINGS_FIELD_DESC = new org.apache.thrift.protocol.TField("warnings", org.apache.thrift.protocol.TType.LIST, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new WMValidateResourcePlanResponseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new WMValidateResourcePlanResponseTupleSchemeFactory());
  }

  private List<String> errors; // optional
  private List<String> warnings; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ERRORS((short)1, "errors"),
    WARNINGS((short)2, "warnings");

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
        case 1: // ERRORS
          return ERRORS;
        case 2: // WARNINGS
          return WARNINGS;
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
  private static final _Fields optionals[] = {_Fields.ERRORS,_Fields.WARNINGS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ERRORS, new org.apache.thrift.meta_data.FieldMetaData("errors", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.WARNINGS, new org.apache.thrift.meta_data.FieldMetaData("warnings", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WMValidateResourcePlanResponse.class, metaDataMap);
  }

  public WMValidateResourcePlanResponse() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WMValidateResourcePlanResponse(WMValidateResourcePlanResponse other) {
    if (other.isSetErrors()) {
      List<String> __this__errors = new ArrayList<String>(other.errors);
      this.errors = __this__errors;
    }
    if (other.isSetWarnings()) {
      List<String> __this__warnings = new ArrayList<String>(other.warnings);
      this.warnings = __this__warnings;
    }
  }

  public WMValidateResourcePlanResponse deepCopy() {
    return new WMValidateResourcePlanResponse(this);
  }

  @Override
  public void clear() {
    this.errors = null;
    this.warnings = null;
  }

  public int getErrorsSize() {
    return (this.errors == null) ? 0 : this.errors.size();
  }

  public java.util.Iterator<String> getErrorsIterator() {
    return (this.errors == null) ? null : this.errors.iterator();
  }

  public void addToErrors(String elem) {
    if (this.errors == null) {
      this.errors = new ArrayList<String>();
    }
    this.errors.add(elem);
  }

  public List<String> getErrors() {
    return this.errors;
  }

  public void setErrors(List<String> errors) {
    this.errors = errors;
  }

  public void unsetErrors() {
    this.errors = null;
  }

  /** Returns true if field errors is set (has been assigned a value) and false otherwise */
  public boolean isSetErrors() {
    return this.errors != null;
  }

  public void setErrorsIsSet(boolean value) {
    if (!value) {
      this.errors = null;
    }
  }

  public int getWarningsSize() {
    return (this.warnings == null) ? 0 : this.warnings.size();
  }

  public java.util.Iterator<String> getWarningsIterator() {
    return (this.warnings == null) ? null : this.warnings.iterator();
  }

  public void addToWarnings(String elem) {
    if (this.warnings == null) {
      this.warnings = new ArrayList<String>();
    }
    this.warnings.add(elem);
  }

  public List<String> getWarnings() {
    return this.warnings;
  }

  public void setWarnings(List<String> warnings) {
    this.warnings = warnings;
  }

  public void unsetWarnings() {
    this.warnings = null;
  }

  /** Returns true if field warnings is set (has been assigned a value) and false otherwise */
  public boolean isSetWarnings() {
    return this.warnings != null;
  }

  public void setWarningsIsSet(boolean value) {
    if (!value) {
      this.warnings = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ERRORS:
      if (value == null) {
        unsetErrors();
      } else {
        setErrors((List<String>)value);
      }
      break;

    case WARNINGS:
      if (value == null) {
        unsetWarnings();
      } else {
        setWarnings((List<String>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ERRORS:
      return getErrors();

    case WARNINGS:
      return getWarnings();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ERRORS:
      return isSetErrors();
    case WARNINGS:
      return isSetWarnings();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof WMValidateResourcePlanResponse)
      return this.equals((WMValidateResourcePlanResponse)that);
    return false;
  }

  public boolean equals(WMValidateResourcePlanResponse that) {
    if (that == null)
      return false;

    boolean this_present_errors = true && this.isSetErrors();
    boolean that_present_errors = true && that.isSetErrors();
    if (this_present_errors || that_present_errors) {
      if (!(this_present_errors && that_present_errors))
        return false;
      if (!this.errors.equals(that.errors))
        return false;
    }

    boolean this_present_warnings = true && this.isSetWarnings();
    boolean that_present_warnings = true && that.isSetWarnings();
    if (this_present_warnings || that_present_warnings) {
      if (!(this_present_warnings && that_present_warnings))
        return false;
      if (!this.warnings.equals(that.warnings))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_errors = true && (isSetErrors());
    list.add(present_errors);
    if (present_errors)
      list.add(errors);

    boolean present_warnings = true && (isSetWarnings());
    list.add(present_warnings);
    if (present_warnings)
      list.add(warnings);

    return list.hashCode();
  }

  @Override
  public int compareTo(WMValidateResourcePlanResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetErrors()).compareTo(other.isSetErrors());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetErrors()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.errors, other.errors);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetWarnings()).compareTo(other.isSetWarnings());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWarnings()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.warnings, other.warnings);
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
    StringBuilder sb = new StringBuilder("WMValidateResourcePlanResponse(");
    boolean first = true;

    if (isSetErrors()) {
      sb.append("errors:");
      if (this.errors == null) {
        sb.append("null");
      } else {
        sb.append(this.errors);
      }
      first = false;
    }
    if (isSetWarnings()) {
      if (!first) sb.append(", ");
      sb.append("warnings:");
      if (this.warnings == null) {
        sb.append("null");
      } else {
        sb.append(this.warnings);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

  private static class WMValidateResourcePlanResponseStandardSchemeFactory implements SchemeFactory {
    public WMValidateResourcePlanResponseStandardScheme getScheme() {
      return new WMValidateResourcePlanResponseStandardScheme();
    }
  }

  private static class WMValidateResourcePlanResponseStandardScheme extends StandardScheme<WMValidateResourcePlanResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, WMValidateResourcePlanResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ERRORS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list992 = iprot.readListBegin();
                struct.errors = new ArrayList<String>(_list992.size);
                String _elem993;
                for (int _i994 = 0; _i994 < _list992.size; ++_i994)
                {
                  _elem993 = iprot.readString();
                  struct.errors.add(_elem993);
                }
                iprot.readListEnd();
              }
              struct.setErrorsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // WARNINGS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list995 = iprot.readListBegin();
                struct.warnings = new ArrayList<String>(_list995.size);
                String _elem996;
                for (int _i997 = 0; _i997 < _list995.size; ++_i997)
                {
                  _elem996 = iprot.readString();
                  struct.warnings.add(_elem996);
                }
                iprot.readListEnd();
              }
              struct.setWarningsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, WMValidateResourcePlanResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.errors != null) {
        if (struct.isSetErrors()) {
          oprot.writeFieldBegin(ERRORS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.errors.size()));
            for (String _iter998 : struct.errors)
            {
              oprot.writeString(_iter998);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.warnings != null) {
        if (struct.isSetWarnings()) {
          oprot.writeFieldBegin(WARNINGS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.warnings.size()));
            for (String _iter999 : struct.warnings)
            {
              oprot.writeString(_iter999);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class WMValidateResourcePlanResponseTupleSchemeFactory implements SchemeFactory {
    public WMValidateResourcePlanResponseTupleScheme getScheme() {
      return new WMValidateResourcePlanResponseTupleScheme();
    }
  }

  private static class WMValidateResourcePlanResponseTupleScheme extends TupleScheme<WMValidateResourcePlanResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, WMValidateResourcePlanResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetErrors()) {
        optionals.set(0);
      }
      if (struct.isSetWarnings()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetErrors()) {
        {
          oprot.writeI32(struct.errors.size());
          for (String _iter1000 : struct.errors)
          {
            oprot.writeString(_iter1000);
          }
        }
      }
      if (struct.isSetWarnings()) {
        {
          oprot.writeI32(struct.warnings.size());
          for (String _iter1001 : struct.warnings)
          {
            oprot.writeString(_iter1001);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, WMValidateResourcePlanResponse struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list1002 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.errors = new ArrayList<String>(_list1002.size);
          String _elem1003;
          for (int _i1004 = 0; _i1004 < _list1002.size; ++_i1004)
          {
            _elem1003 = iprot.readString();
            struct.errors.add(_elem1003);
          }
        }
        struct.setErrorsIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list1005 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.warnings = new ArrayList<String>(_list1005.size);
          String _elem1006;
          for (int _i1007 = 0; _i1007 < _list1005.size; ++_i1007)
          {
            _elem1006 = iprot.readString();
            struct.warnings.add(_elem1006);
          }
        }
        struct.setWarningsIsSet(true);
      }
    }
  }

}

