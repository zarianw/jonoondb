// automatically generated by the FlatBuffers compiler, do not modify

#ifndef FLATBUFFERS_GENERATED_ALLFIELDTYPE_JONOONDB_TEST_H_
#define FLATBUFFERS_GENERATED_ALLFIELDTYPE_JONOONDB_TEST_H_

#include "flatbuffers/flatbuffers.h"


namespace jonoondb_test {

struct StructType;
struct UnionTypeA;
struct UnionTypeB;
struct NestedAllFieldType;
struct AllFieldType;

enum UnionType {
  UnionType_NONE = 0,
  UnionType_UnionTypeA = 1,
  UnionType_UnionTypeB = 2,
  UnionType_MIN = UnionType_NONE,
  UnionType_MAX = UnionType_UnionTypeB
};

inline const char **EnumNamesUnionType() {
  static const char *names[] = { "NONE", "UnionTypeA", "UnionTypeB", nullptr };
  return names;
}

inline const char *EnumNameUnionType(UnionType e) { return EnumNamesUnionType()[static_cast<int>(e)]; }

inline bool VerifyUnionType(flatbuffers::Verifier &verifier, const void *union_obj, UnionType type);

MANUALLY_ALIGNED_STRUCT(1) StructType FLATBUFFERS_FINAL_CLASS {
 private:
  int8_t field1_;
  uint8_t field2_;

 public:
  StructType(int8_t _field1, uint8_t _field2)
    : field1_(flatbuffers::EndianScalar(_field1)), field2_(flatbuffers::EndianScalar(_field2)) { }

  int8_t field1() const { return flatbuffers::EndianScalar(field1_); }
  uint8_t field2() const { return flatbuffers::EndianScalar(field2_); }
};
STRUCT_END(StructType, 2);

struct UnionTypeA FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum {
    VT_FIELD1 = 4,
    VT_FIELD2 = 6
  };
  int8_t field1() const { return GetField<int8_t>(VT_FIELD1, 0); }
  uint8_t field2() const { return GetField<uint8_t>(VT_FIELD2, 0); }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int8_t>(verifier, VT_FIELD1) &&
           VerifyField<uint8_t>(verifier, VT_FIELD2) &&
           verifier.EndTable();
  }
};

struct UnionTypeABuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_field1(int8_t field1) { fbb_.AddElement<int8_t>(UnionTypeA::VT_FIELD1, field1, 0); }
  void add_field2(uint8_t field2) { fbb_.AddElement<uint8_t>(UnionTypeA::VT_FIELD2, field2, 0); }
  UnionTypeABuilder(flatbuffers::FlatBufferBuilder &_fbb) : fbb_(_fbb) { start_ = fbb_.StartTable(); }
  UnionTypeABuilder &operator=(const UnionTypeABuilder &);
  flatbuffers::Offset<UnionTypeA> Finish() {
    auto o = flatbuffers::Offset<UnionTypeA>(fbb_.EndTable(start_, 2));
    return o;
  }
};

inline flatbuffers::Offset<UnionTypeA> CreateUnionTypeA(flatbuffers::FlatBufferBuilder &_fbb,
   int8_t field1 = 0,
   uint8_t field2 = 0) {
  UnionTypeABuilder builder_(_fbb);
  builder_.add_field2(field2);
  builder_.add_field1(field1);
  return builder_.Finish();
}

struct UnionTypeB FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum {
    VT_FIELD3 = 4,
    VT_FIELD4 = 6
  };
  bool field3() const { return GetField<uint8_t>(VT_FIELD3, 0) != 0; }
  int16_t field4() const { return GetField<int16_t>(VT_FIELD4, 0); }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<uint8_t>(verifier, VT_FIELD3) &&
           VerifyField<int16_t>(verifier, VT_FIELD4) &&
           verifier.EndTable();
  }
};

struct UnionTypeBBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_field3(bool field3) { fbb_.AddElement<uint8_t>(UnionTypeB::VT_FIELD3, static_cast<uint8_t>(field3), 0); }
  void add_field4(int16_t field4) { fbb_.AddElement<int16_t>(UnionTypeB::VT_FIELD4, field4, 0); }
  UnionTypeBBuilder(flatbuffers::FlatBufferBuilder &_fbb) : fbb_(_fbb) { start_ = fbb_.StartTable(); }
  UnionTypeBBuilder &operator=(const UnionTypeBBuilder &);
  flatbuffers::Offset<UnionTypeB> Finish() {
    auto o = flatbuffers::Offset<UnionTypeB>(fbb_.EndTable(start_, 2));
    return o;
  }
};

inline flatbuffers::Offset<UnionTypeB> CreateUnionTypeB(flatbuffers::FlatBufferBuilder &_fbb,
   bool field3 = false,
   int16_t field4 = 0) {
  UnionTypeBBuilder builder_(_fbb);
  builder_.add_field4(field4);
  builder_.add_field3(field3);
  return builder_.Finish();
}

struct NestedAllFieldType FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum {
    VT_FIELD1 = 4,
    VT_FIELD2 = 6,
    VT_FIELD3 = 8,
    VT_FIELD4 = 10,
    VT_FIELD5 = 12,
    VT_FIELD6 = 14,
    VT_FIELD7 = 16,
    VT_FIELD8 = 18,
    VT_FIELD9 = 20,
    VT_FIELD10 = 22,
    VT_FIELD11 = 24,
    VT_FIELD12 = 26,
    VT_FIELD13 = 28,
    VT_FIELD14 = 30,
    VT_FIELD15 = 32,
    VT_FIELD16 = 34,
    VT_FIELD17_TYPE = 36,
    VT_FIELD17 = 38,
    VT_FIELD18_TYPE = 40,
    VT_FIELD18 = 42
  };
  int8_t field1() const { return GetField<int8_t>(VT_FIELD1, 0); }
  uint8_t field2() const { return GetField<uint8_t>(VT_FIELD2, 0); }
  bool field3() const { return GetField<uint8_t>(VT_FIELD3, 0) != 0; }
  int16_t field4() const { return GetField<int16_t>(VT_FIELD4, 0); }
  uint16_t field5() const { return GetField<uint16_t>(VT_FIELD5, 0); }
  int32_t field6() const { return GetField<int32_t>(VT_FIELD6, 0); }
  uint32_t field7() const { return GetField<uint32_t>(VT_FIELD7, 0); }
  float field8() const { return GetField<float>(VT_FIELD8, 0); }
  int64_t field9() const { return GetField<int64_t>(VT_FIELD9, 0); }
  double field10() const { return GetField<double>(VT_FIELD10, 0); }
  const flatbuffers::String *field11() const { return GetPointer<const flatbuffers::String *>(VT_FIELD11); }
  const flatbuffers::Vector<int8_t> *field12() const { return GetPointer<const flatbuffers::Vector<int8_t> *>(VT_FIELD12); }
  const flatbuffers::Vector<uint8_t> *field13() const { return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_FIELD13); }
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *field14() const { return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *>(VT_FIELD14); }
  const StructType *field15() const { return GetStruct<const StructType *>(VT_FIELD15); }
  const flatbuffers::Vector<const StructType *> *field16() const { return GetPointer<const flatbuffers::Vector<const StructType *> *>(VT_FIELD16); }
  UnionType field17_type() const { return static_cast<UnionType>(GetField<uint8_t>(VT_FIELD17_TYPE, 0)); }
  const void *field17() const { return GetPointer<const void *>(VT_FIELD17); }
  UnionType field18_type() const { return static_cast<UnionType>(GetField<uint8_t>(VT_FIELD18_TYPE, 0)); }
  const void *field18() const { return GetPointer<const void *>(VT_FIELD18); }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int8_t>(verifier, VT_FIELD1) &&
           VerifyField<uint8_t>(verifier, VT_FIELD2) &&
           VerifyField<uint8_t>(verifier, VT_FIELD3) &&
           VerifyField<int16_t>(verifier, VT_FIELD4) &&
           VerifyField<uint16_t>(verifier, VT_FIELD5) &&
           VerifyField<int32_t>(verifier, VT_FIELD6) &&
           VerifyField<uint32_t>(verifier, VT_FIELD7) &&
           VerifyField<float>(verifier, VT_FIELD8) &&
           VerifyField<int64_t>(verifier, VT_FIELD9) &&
           VerifyField<double>(verifier, VT_FIELD10) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD11) &&
           verifier.Verify(field11()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD12) &&
           verifier.Verify(field12()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD13) &&
           verifier.Verify(field13()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD14) &&
           verifier.Verify(field14()) &&
           verifier.VerifyVectorOfStrings(field14()) &&
           VerifyField<StructType>(verifier, VT_FIELD15) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD16) &&
           verifier.Verify(field16()) &&
           VerifyField<uint8_t>(verifier, VT_FIELD17_TYPE) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD17) &&
           VerifyUnionType(verifier, field17(), field17_type()) &&
           VerifyField<uint8_t>(verifier, VT_FIELD18_TYPE) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD18) &&
           VerifyUnionType(verifier, field18(), field18_type()) &&
           verifier.EndTable();
  }
};

struct NestedAllFieldTypeBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_field1(int8_t field1) { fbb_.AddElement<int8_t>(NestedAllFieldType::VT_FIELD1, field1, 0); }
  void add_field2(uint8_t field2) { fbb_.AddElement<uint8_t>(NestedAllFieldType::VT_FIELD2, field2, 0); }
  void add_field3(bool field3) { fbb_.AddElement<uint8_t>(NestedAllFieldType::VT_FIELD3, static_cast<uint8_t>(field3), 0); }
  void add_field4(int16_t field4) { fbb_.AddElement<int16_t>(NestedAllFieldType::VT_FIELD4, field4, 0); }
  void add_field5(uint16_t field5) { fbb_.AddElement<uint16_t>(NestedAllFieldType::VT_FIELD5, field5, 0); }
  void add_field6(int32_t field6) { fbb_.AddElement<int32_t>(NestedAllFieldType::VT_FIELD6, field6, 0); }
  void add_field7(uint32_t field7) { fbb_.AddElement<uint32_t>(NestedAllFieldType::VT_FIELD7, field7, 0); }
  void add_field8(float field8) { fbb_.AddElement<float>(NestedAllFieldType::VT_FIELD8, field8, 0); }
  void add_field9(int64_t field9) { fbb_.AddElement<int64_t>(NestedAllFieldType::VT_FIELD9, field9, 0); }
  void add_field10(double field10) { fbb_.AddElement<double>(NestedAllFieldType::VT_FIELD10, field10, 0); }
  void add_field11(flatbuffers::Offset<flatbuffers::String> field11) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD11, field11); }
  void add_field12(flatbuffers::Offset<flatbuffers::Vector<int8_t>> field12) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD12, field12); }
  void add_field13(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> field13) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD13, field13); }
  void add_field14(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>> field14) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD14, field14); }
  void add_field15(const StructType *field15) { fbb_.AddStruct(NestedAllFieldType::VT_FIELD15, field15); }
  void add_field16(flatbuffers::Offset<flatbuffers::Vector<const StructType *>> field16) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD16, field16); }
  void add_field17_type(UnionType field17_type) { fbb_.AddElement<uint8_t>(NestedAllFieldType::VT_FIELD17_TYPE, static_cast<uint8_t>(field17_type), 0); }
  void add_field17(flatbuffers::Offset<void> field17) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD17, field17); }
  void add_field18_type(UnionType field18_type) { fbb_.AddElement<uint8_t>(NestedAllFieldType::VT_FIELD18_TYPE, static_cast<uint8_t>(field18_type), 0); }
  void add_field18(flatbuffers::Offset<void> field18) { fbb_.AddOffset(NestedAllFieldType::VT_FIELD18, field18); }
  NestedAllFieldTypeBuilder(flatbuffers::FlatBufferBuilder &_fbb) : fbb_(_fbb) { start_ = fbb_.StartTable(); }
  NestedAllFieldTypeBuilder &operator=(const NestedAllFieldTypeBuilder &);
  flatbuffers::Offset<NestedAllFieldType> Finish() {
    auto o = flatbuffers::Offset<NestedAllFieldType>(fbb_.EndTable(start_, 20));
    return o;
  }
};

inline flatbuffers::Offset<NestedAllFieldType> CreateNestedAllFieldType(flatbuffers::FlatBufferBuilder &_fbb,
   int8_t field1 = 0,
   uint8_t field2 = 0,
   bool field3 = false,
   int16_t field4 = 0,
   uint16_t field5 = 0,
   int32_t field6 = 0,
   uint32_t field7 = 0,
   float field8 = 0,
   int64_t field9 = 0,
   double field10 = 0,
   flatbuffers::Offset<flatbuffers::String> field11 = 0,
   flatbuffers::Offset<flatbuffers::Vector<int8_t>> field12 = 0,
   flatbuffers::Offset<flatbuffers::Vector<uint8_t>> field13 = 0,
   flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>> field14 = 0,
   const StructType *field15 = 0,
   flatbuffers::Offset<flatbuffers::Vector<const StructType *>> field16 = 0,
   UnionType field17_type = UnionType_NONE,
   flatbuffers::Offset<void> field17 = 0,
   UnionType field18_type = UnionType_NONE,
   flatbuffers::Offset<void> field18 = 0) {
  NestedAllFieldTypeBuilder builder_(_fbb);
  builder_.add_field10(field10);
  builder_.add_field9(field9);
  builder_.add_field18(field18);
  builder_.add_field17(field17);
  builder_.add_field16(field16);
  builder_.add_field15(field15);
  builder_.add_field14(field14);
  builder_.add_field13(field13);
  builder_.add_field12(field12);
  builder_.add_field11(field11);
  builder_.add_field8(field8);
  builder_.add_field7(field7);
  builder_.add_field6(field6);
  builder_.add_field5(field5);
  builder_.add_field4(field4);
  builder_.add_field18_type(field18_type);
  builder_.add_field17_type(field17_type);
  builder_.add_field3(field3);
  builder_.add_field2(field2);
  builder_.add_field1(field1);
  return builder_.Finish();
}

struct AllFieldType FLATBUFFERS_FINAL_CLASS : private flatbuffers::Table {
  enum {
    VT_FIELD1 = 4,
    VT_FIELD2 = 6,
    VT_FIELD3 = 8,
    VT_FIELD4 = 10,
    VT_FIELD5 = 12,
    VT_FIELD6 = 14,
    VT_FIELD7 = 16,
    VT_FIELD8 = 18,
    VT_FIELD9 = 20,
    VT_FIELD10 = 22,
    VT_FIELD11 = 24,
    VT_NESTEDFIELD = 26,
    VT_FIELD12 = 28,
    VT_FIELD13 = 30,
    VT_FIELD14 = 32,
    VT_FIELD15 = 34,
    VT_FIELD16 = 36,
    VT_FIELD17_TYPE = 38,
    VT_FIELD17 = 40,
    VT_FIELD18_TYPE = 42,
    VT_FIELD18 = 44,
    VT_NESTEDFIELDVECTOR = 46
  };
  int8_t field1() const { return GetField<int8_t>(VT_FIELD1, 0); }
  uint8_t field2() const { return GetField<uint8_t>(VT_FIELD2, 0); }
  bool field3() const { return GetField<uint8_t>(VT_FIELD3, 0) != 0; }
  int16_t field4() const { return GetField<int16_t>(VT_FIELD4, 0); }
  uint16_t field5() const { return GetField<uint16_t>(VT_FIELD5, 0); }
  int32_t field6() const { return GetField<int32_t>(VT_FIELD6, 0); }
  uint32_t field7() const { return GetField<uint32_t>(VT_FIELD7, 0); }
  float field8() const { return GetField<float>(VT_FIELD8, 0); }
  int64_t field9() const { return GetField<int64_t>(VT_FIELD9, 0); }
  double field10() const { return GetField<double>(VT_FIELD10, 0); }
  const flatbuffers::String *field11() const { return GetPointer<const flatbuffers::String *>(VT_FIELD11); }
  const NestedAllFieldType *nestedField() const { return GetPointer<const NestedAllFieldType *>(VT_NESTEDFIELD); }
  const flatbuffers::Vector<int8_t> *field12() const { return GetPointer<const flatbuffers::Vector<int8_t> *>(VT_FIELD12); }
  const flatbuffers::Vector<uint8_t> *field13() const { return GetPointer<const flatbuffers::Vector<uint8_t> *>(VT_FIELD13); }
  const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *field14() const { return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>> *>(VT_FIELD14); }
  const StructType *field15() const { return GetStruct<const StructType *>(VT_FIELD15); }
  const flatbuffers::Vector<const StructType *> *field16() const { return GetPointer<const flatbuffers::Vector<const StructType *> *>(VT_FIELD16); }
  UnionType field17_type() const { return static_cast<UnionType>(GetField<uint8_t>(VT_FIELD17_TYPE, 0)); }
  const void *field17() const { return GetPointer<const void *>(VT_FIELD17); }
  UnionType field18_type() const { return static_cast<UnionType>(GetField<uint8_t>(VT_FIELD18_TYPE, 0)); }
  const void *field18() const { return GetPointer<const void *>(VT_FIELD18); }
  const flatbuffers::Vector<flatbuffers::Offset<NestedAllFieldType>> *nestedFieldVector() const { return GetPointer<const flatbuffers::Vector<flatbuffers::Offset<NestedAllFieldType>> *>(VT_NESTEDFIELDVECTOR); }
  bool Verify(flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) &&
           VerifyField<int8_t>(verifier, VT_FIELD1) &&
           VerifyField<uint8_t>(verifier, VT_FIELD2) &&
           VerifyField<uint8_t>(verifier, VT_FIELD3) &&
           VerifyField<int16_t>(verifier, VT_FIELD4) &&
           VerifyField<uint16_t>(verifier, VT_FIELD5) &&
           VerifyField<int32_t>(verifier, VT_FIELD6) &&
           VerifyField<uint32_t>(verifier, VT_FIELD7) &&
           VerifyField<float>(verifier, VT_FIELD8) &&
           VerifyField<int64_t>(verifier, VT_FIELD9) &&
           VerifyField<double>(verifier, VT_FIELD10) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD11) &&
           verifier.Verify(field11()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_NESTEDFIELD) &&
           verifier.VerifyTable(nestedField()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD12) &&
           verifier.Verify(field12()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD13) &&
           verifier.Verify(field13()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD14) &&
           verifier.Verify(field14()) &&
           verifier.VerifyVectorOfStrings(field14()) &&
           VerifyField<StructType>(verifier, VT_FIELD15) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD16) &&
           verifier.Verify(field16()) &&
           VerifyField<uint8_t>(verifier, VT_FIELD17_TYPE) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD17) &&
           VerifyUnionType(verifier, field17(), field17_type()) &&
           VerifyField<uint8_t>(verifier, VT_FIELD18_TYPE) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_FIELD18) &&
           VerifyUnionType(verifier, field18(), field18_type()) &&
           VerifyField<flatbuffers::uoffset_t>(verifier, VT_NESTEDFIELDVECTOR) &&
           verifier.Verify(nestedFieldVector()) &&
           verifier.VerifyVectorOfTables(nestedFieldVector()) &&
           verifier.EndTable();
  }
};

struct AllFieldTypeBuilder {
  flatbuffers::FlatBufferBuilder &fbb_;
  flatbuffers::uoffset_t start_;
  void add_field1(int8_t field1) { fbb_.AddElement<int8_t>(AllFieldType::VT_FIELD1, field1, 0); }
  void add_field2(uint8_t field2) { fbb_.AddElement<uint8_t>(AllFieldType::VT_FIELD2, field2, 0); }
  void add_field3(bool field3) { fbb_.AddElement<uint8_t>(AllFieldType::VT_FIELD3, static_cast<uint8_t>(field3), 0); }
  void add_field4(int16_t field4) { fbb_.AddElement<int16_t>(AllFieldType::VT_FIELD4, field4, 0); }
  void add_field5(uint16_t field5) { fbb_.AddElement<uint16_t>(AllFieldType::VT_FIELD5, field5, 0); }
  void add_field6(int32_t field6) { fbb_.AddElement<int32_t>(AllFieldType::VT_FIELD6, field6, 0); }
  void add_field7(uint32_t field7) { fbb_.AddElement<uint32_t>(AllFieldType::VT_FIELD7, field7, 0); }
  void add_field8(float field8) { fbb_.AddElement<float>(AllFieldType::VT_FIELD8, field8, 0); }
  void add_field9(int64_t field9) { fbb_.AddElement<int64_t>(AllFieldType::VT_FIELD9, field9, 0); }
  void add_field10(double field10) { fbb_.AddElement<double>(AllFieldType::VT_FIELD10, field10, 0); }
  void add_field11(flatbuffers::Offset<flatbuffers::String> field11) { fbb_.AddOffset(AllFieldType::VT_FIELD11, field11); }
  void add_nestedField(flatbuffers::Offset<NestedAllFieldType> nestedField) { fbb_.AddOffset(AllFieldType::VT_NESTEDFIELD, nestedField); }
  void add_field12(flatbuffers::Offset<flatbuffers::Vector<int8_t>> field12) { fbb_.AddOffset(AllFieldType::VT_FIELD12, field12); }
  void add_field13(flatbuffers::Offset<flatbuffers::Vector<uint8_t>> field13) { fbb_.AddOffset(AllFieldType::VT_FIELD13, field13); }
  void add_field14(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>> field14) { fbb_.AddOffset(AllFieldType::VT_FIELD14, field14); }
  void add_field15(const StructType *field15) { fbb_.AddStruct(AllFieldType::VT_FIELD15, field15); }
  void add_field16(flatbuffers::Offset<flatbuffers::Vector<const StructType *>> field16) { fbb_.AddOffset(AllFieldType::VT_FIELD16, field16); }
  void add_field17_type(UnionType field17_type) { fbb_.AddElement<uint8_t>(AllFieldType::VT_FIELD17_TYPE, static_cast<uint8_t>(field17_type), 0); }
  void add_field17(flatbuffers::Offset<void> field17) { fbb_.AddOffset(AllFieldType::VT_FIELD17, field17); }
  void add_field18_type(UnionType field18_type) { fbb_.AddElement<uint8_t>(AllFieldType::VT_FIELD18_TYPE, static_cast<uint8_t>(field18_type), 0); }
  void add_field18(flatbuffers::Offset<void> field18) { fbb_.AddOffset(AllFieldType::VT_FIELD18, field18); }
  void add_nestedFieldVector(flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<NestedAllFieldType>>> nestedFieldVector) { fbb_.AddOffset(AllFieldType::VT_NESTEDFIELDVECTOR, nestedFieldVector); }
  AllFieldTypeBuilder(flatbuffers::FlatBufferBuilder &_fbb) : fbb_(_fbb) { start_ = fbb_.StartTable(); }
  AllFieldTypeBuilder &operator=(const AllFieldTypeBuilder &);
  flatbuffers::Offset<AllFieldType> Finish() {
    auto o = flatbuffers::Offset<AllFieldType>(fbb_.EndTable(start_, 22));
    return o;
  }
};

inline flatbuffers::Offset<AllFieldType> CreateAllFieldType(flatbuffers::FlatBufferBuilder &_fbb,
   int8_t field1 = 0,
   uint8_t field2 = 0,
   bool field3 = false,
   int16_t field4 = 0,
   uint16_t field5 = 0,
   int32_t field6 = 0,
   uint32_t field7 = 0,
   float field8 = 0,
   int64_t field9 = 0,
   double field10 = 0,
   flatbuffers::Offset<flatbuffers::String> field11 = 0,
   flatbuffers::Offset<NestedAllFieldType> nestedField = 0,
   flatbuffers::Offset<flatbuffers::Vector<int8_t>> field12 = 0,
   flatbuffers::Offset<flatbuffers::Vector<uint8_t>> field13 = 0,
   flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>> field14 = 0,
   const StructType *field15 = 0,
   flatbuffers::Offset<flatbuffers::Vector<const StructType *>> field16 = 0,
   UnionType field17_type = UnionType_NONE,
   flatbuffers::Offset<void> field17 = 0,
   UnionType field18_type = UnionType_NONE,
   flatbuffers::Offset<void> field18 = 0,
   flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<NestedAllFieldType>>> nestedFieldVector = 0) {
  AllFieldTypeBuilder builder_(_fbb);
  builder_.add_field10(field10);
  builder_.add_field9(field9);
  builder_.add_nestedFieldVector(nestedFieldVector);
  builder_.add_field18(field18);
  builder_.add_field17(field17);
  builder_.add_field16(field16);
  builder_.add_field15(field15);
  builder_.add_field14(field14);
  builder_.add_field13(field13);
  builder_.add_field12(field12);
  builder_.add_nestedField(nestedField);
  builder_.add_field11(field11);
  builder_.add_field8(field8);
  builder_.add_field7(field7);
  builder_.add_field6(field6);
  builder_.add_field5(field5);
  builder_.add_field4(field4);
  builder_.add_field18_type(field18_type);
  builder_.add_field17_type(field17_type);
  builder_.add_field3(field3);
  builder_.add_field2(field2);
  builder_.add_field1(field1);
  return builder_.Finish();
}

inline bool VerifyUnionType(flatbuffers::Verifier &verifier, const void *union_obj, UnionType type) {
  switch (type) {
    case UnionType_NONE: return true;
    case UnionType_UnionTypeA: return verifier.VerifyTable(reinterpret_cast<const UnionTypeA *>(union_obj));
    case UnionType_UnionTypeB: return verifier.VerifyTable(reinterpret_cast<const UnionTypeB *>(union_obj));
    default: return false;
  }
}

inline const jonoondb_test::AllFieldType *GetAllFieldType(const void *buf) { return flatbuffers::GetRoot<jonoondb_test::AllFieldType>(buf); }

inline bool VerifyAllFieldTypeBuffer(flatbuffers::Verifier &verifier) { return verifier.VerifyBuffer<jonoondb_test::AllFieldType>(); }

inline const char *AllFieldTypeExtension() { return "bfbs"; }

inline void FinishAllFieldTypeBuffer(flatbuffers::FlatBufferBuilder &fbb, flatbuffers::Offset<jonoondb_test::AllFieldType> root) { fbb.Finish(root); }

}  // namespace jonoondb_test

#endif  // FLATBUFFERS_GENERATED_ALLFIELDTYPE_JONOONDB_TEST_H_
