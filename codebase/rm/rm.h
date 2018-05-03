
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

typedef struct AttrwithPos{
    int32_t pos;
    Attribute attr;
} AttrwithPos;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

  friend class RelationManger;

private:
    RBFM_ScanIterator rbfm_scanIterator;
    FileHandle fileHandle;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();
  // create two system catalog tables: tables and columns
    // if they already exist, error
    //actual files for these two tables should be created,
    //tuples descriping these two system catalog tables themselves should be inserted to tables
  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  const vector<Attribute> tableDescriptor;
  const vector<Attribute> columnDescriptor;

  static vector<Attribute> createTableDescriptor();
  static vector<Attribute> createColumnDescriptor();

  RC insertColumns (int32_t table_id, const vector<Attribute> &recordDescriptor);
  RC insertTables (int32_t table_id, bool system, const string &tableName);
  RC getNextTableId(int32_t &id);
  RC getTableId(const string &tableName, int32_t &tableid);

};

#endif
