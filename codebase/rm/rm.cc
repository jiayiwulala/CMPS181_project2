
#include "rm.h"
#include <iostream>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

vector<Attribute> RelationManager::createTableDescriptor() {
    vector<Attribute> tableDescriptor;

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    tableDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    tableDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    tableDescriptor.push_back(attr);

    attr.name = "system"; // 1 has access , 0 not
    attr.type = TypeInt;
    attr.length = (AttrLength) INT_SIZE;
    tableDescriptor.push_back(attr);
    return tableDescriptor;

}


vector<Attribute> RelationManager::createColumnDescriptor(){
        vector<Attribute> columnDescriptor;

        Attribute attr;
        attr.name = "table-id";
        attr.type = TypeInt;
        attr.length = (AttrLength)INT_SIZE;
        columnDescriptor.push_back(attr);

        attr.name = "column-name";
        attr.type = TypeVarChar;
        attr.length = 50;
        columnDescriptor.push_back(attr);

        attr.name = "column-type";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnDescriptor.push_back(attr);

        attr.name = "column-length";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnDescriptor.push_back(attr);

        attr.name = "column-position";
        attr.type = TypeInt;
        attr.length = (AttrLength) INT_SIZE;
        columnDescriptor.push_back(attr);

        return columnDescriptor;
}

//insert the given columns to Column Table
RC RelationManager::insertColumns (int32_t table_id, const vector<Attribute> &recordDescriptor) {
    FileHandle fileHandle;
    RID rid;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile("Columns.ext", fileHandle);
    if (rc) return rc;

    void * columndata = malloc(1+4+4+50+4+4+4); //null +
    // create columns column entry for the given id and Tabledecriptor/Columndescripter/recorddescriptor
    for (unsigned i = 0; i < recordDescriptor.size(); i++){
        Attribute attr = recordDescriptor[i];
        unsigned offset = 0; //reset
        int32_t attrnameLen = attr.name.length();

        char nullInd = 0;
        memcpy((char *) columndata + offset, &nullInd, 1);
        offset += 1;
        //read table id
        memcpy((char *) columndata + offset, &table_id, INT_SIZE);
        offset += INT_SIZE;
        //read column-name
        memcpy((char *) columndata + offset, &attrnameLen, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        memcpy((char *) columndata + offset, attr.name.c_str(), attrnameLen);
        offset += attrnameLen;
        //read column type
        int32_t type = attr.type;
        memcpy((char *) columndata + offset, &type, INT_SIZE);
        offset += INT_SIZE;
        //read column length
        int32_t leng = attr.length;
        memcpy((char *) columndata + offset, &leng, INT_SIZE);
        offset += INT_SIZE;
        //read column position
        int32_t pos = i + 1;
        memcpy((char *) columndata + offset, &pos, INT_SIZE); //&(i+1)??
        offset += INT_SIZE;

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, columndata, rid);
        if (rc) return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columndata);
    return SUCCESS;
}

//insert to table to Tables
RC RelationManager::insertTables (int32_t table_id, bool system, const string &tableName) {
    FileHandle fileHandle;
    RID rid;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->openFile("Tables.ext", fileHandle);
    if (rc) return rc;

    void * tabledata = malloc(1+4+4+50+50+4+4); //null + 2 int + 2 varchar + 2 int
    // create tables table entry for the given id and tableName
    unsigned offset = 0;
    int32_t tablenameLen = tableName.length();
    string filename = tableName + ".ext";
    int32_t filenameLen = filename.length();
    int32_t accessSystem = system ? 1: 0;

    char null = 0; //set entry not null
    memcpy((char *) tabledata + offset, &null, 1);
    offset += 1;

    //read table id
    memcpy((char *) tabledata + offset, &table_id, INT_SIZE);
    offset += INT_SIZE;
    //read table_name, it length first
    memcpy((char *) tabledata + offset, &tablenameLen, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *) tabledata + offset, tableName.c_str(), tablenameLen);
    offset += tablenameLen;
    //read file-name, length first
    memcpy((char *) tabledata + offset, &filenameLen, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char *) tabledata + offset, filename.c_str(), filenameLen);
    offset += filenameLen;
    // read system indicator
    memcpy((char *) tabledata + offset, &accessSystem, INT_SIZE);
    offset += INT_SIZE;

    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tabledata, rid);
    if (rc) return rc;

    rbfm->closeFile(fileHandle);
    free(tabledata);
    return SUCCESS;
}


RelationManager::RelationManager() : tableDescriptor (createTableDescriptor()), columnDescriptor (createColumnDescriptor())
{
}

RelationManager::~RelationManager()
{
}



RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    //create two files and return error if exit
    RC rc = rbfm->createFile("Tables.ext");
    if (rc) {
        return rc;
    }
    rc = rbfm->createFile("Columns.ext");
    if (rc){
        return rc;
    }

    rc = insertTables(1, true, "Tables");
    if (rc) {
        return rc;
    }

    rc = insertTables(2, true, "Columns");
    if (rc) {
        return rc;
    }

    rc = insertColumns(1, tableDescriptor);
    if (rc) return rc;

    rc = insertColumns(2, columnDescriptor);
    if (rc) return rc;

    return SUCCESS;

}
//delete system catalog tables, delete actual file, return error is system catalog tables do not exist
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->destroyFile("Tables.ext");
    if (rc) return rc;
    rc = rbfm->destroyFile("Columns.ext");
    if (rc) return rc;
    return SUCCESS;
}

RC RelationManager::getTableId(const string &tableName, int32_t &tableid){
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = rbfm->openFile("Tables.ext", fileHandle);
    if (rc) return rc;

    vector<string> attrs;
    attrs.push_back("table-id");

    void * value = malloc(4 + 50);
    int32_t attrnameLen = tableName.length();
    memcpy(value, &attrnameLen, INT_SIZE);
    memcpy((char *) value + INT_SIZE, tableName.c_str(), attrnameLen);

    RBFM_ScanIterator scanIterator;
    rc = rbfm->scan(fileHandle, tableDescriptor, "table-name", EQ_OP, value, attrs, scanIterator);
    if (rc) return rc;

    RID rid;
    void *data = malloc(1 + INT_SIZE);
    if ((rc = scanIterator.getNextRecord(rid, data)) == SUCCESS) {
        char nullInd = 0;
        memcpy(&nullInd, data, 1);
        if (nullInd) {return -1;}
        memcpy(&tableid, (char *) data + 1, INT_SIZE);
    }
    cout << "3333333333" << endl;
    free(data);
    free(value);
    rc = rbfm->closeFile(fileHandle);
    if (rc)
        return rc;
    scanIterator.close();
    return SUCCESS;

}
// scan all table id in file and find the max one, increase by 1
//initiate an scaner, getnext
RC RelationManager::getNextTableId(int32_t &id) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    RC rc = rbfm->openFile("Tables.ext", fileHandle);
    if (rc) return rc;
    vector<string> attrs;
    attrs.push_back("table-id");
    RBFM_ScanIterator scanIterator;
    rc = rbfm->scan(fileHandle, tableDescriptor, "table-id", NO_OP, NULL, attrs, scanIterator);
    if (rc) return rc;
    RID rid;
    void *data = malloc(1 + INT_SIZE);
    int32_t max_tableid = 0;
    while ((rc = scanIterator.getNextRecord(rid, data)) == SUCCESS) {
        //get only table-id from record;
        int32_t tableid;
        char nullInd = 0;
        memcpy(&nullInd, data, 1);
        if (nullInd) {return -1;}
        memcpy(&tableid, (char *) data + 1, INT_SIZE);
        if (tableid > max_tableid) {
            max_tableid = tableid;
        }
    }
    if (rc != RBFM_EOF) {
        return rc;
    }
    free(data);
    id = max_tableid + 1;

    rc = rbfm->closeFile(fileHandle);
    if (rc) return rc;
    scanIterator.close();

    return SUCCESS;
}

//create a table called tableName with a vector of attributes(attr)
//the actual RBF file for this table should be created, return error if tableName already exist
RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC rc = rbfm->createFile(tableName + ".ext");
    if (rc) return rc;

    int32_t id;
    rc = getNextTableId(id);
    if (rc) return rc;

    rc = insertTables(id, 0, tableName);
    if (rc) return rc;
    rc = insertColumns(id, attrs);
    if (rc) return rc;

    return SUCCESS;

}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if (tableName == "Tables" || "Columns") {
        return RM_CANNOT_DELETE_SYS;
    }
//delete file
    RC rc = rbfm->destroyFile(tableName + ".ext");
    if (rc) return rc;
//get tableid so as to delete entry in columns and entry in tables
    int32_t tableid;
    rc = getTableId(tableName, tableid);
    if (rc) return rc;

    //delete entry in table
    FileHandle fileHandle;
    rc = rbfm->openFile("Tables.ext", fileHandle);
    if (rc) return rc;

    RBFM_ScanIterator scanIterator;
    vector<string> attrs; //empty
    void *value = &tableid;

    rc = rbfm->scan(fileHandle, tableDescriptor, "table-id", EQ_OP, value, attrs, scanIterator);
    if (rc) return rc;

    RID rid;
    rc = scanIterator.getNextRecord(rid, NULL);
    if (rc) return rc;

    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    scanIterator.close();

//delete entry in columns
    rc = rbfm->openFile("Columns.ext", fileHandle);
    if (rc) return rc;
    rc = rbfm->scan(fileHandle, columnDescriptor, "table-id", EQ_OP, value, attrs, scanIterator);
    if (rc) return rc;

    while ((rc = scanIterator.getNextRecord(rid, NULL)) == SUCCESS) {
        rc = rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
        if (rc) return rc;
    }
    if (rc != RBFM_EOF) {
        return rc;
    }

    rc = rbfm->closeFile(fileHandle);
    if (rc) return rc;
    scanIterator.close();
    return SUCCESS;
}

//gets attributes of the table by looking in the catalog tables
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    attrs.clear();

    int32_t tableid;
    RC rc = getTableId(tableName, tableid);
    cout << "666666666666" << endl;
    cout << "rc: " <<rc<< endl;
    if (rc) return rc;
    cout << "!!!!!!!!!!!!!!!!" << endl;
    void * value = &tableid;
    cout << "--------"<<tableid << endl;

    FileHandle fileHandle;
    RBFM_ScanIterator scanIterator;
    rc = rbfm->openFile("Columns.ext", fileHandle);
    if (rc) return rc;

    vector<string> attr_show;
    attr_show.push_back("column-name");
    attr_show.push_back("column-type");
    attr_show.push_back("column-length");
    attr_show.push_back("column-position");

    rc = rbfm->scan(fileHandle, columnDescriptor, "table-id", EQ_OP, value, attr_show, scanIterator);
    if (rc) return rc;

    RID rid;
    void *data = malloc(1+4+4+50+4+4+4);
    vector<AttrwithPos> attrP;
    while ((rc = scanIterator.getNextRecord(rid, data)) == SUCCESS){
        AttrwithPos attrp;
        unsigned offset = 0;

        char nullInd;
        memcpy(&nullInd, (char *) data + offset, 1);
        if (nullInd) return -1;
        offset += 1;

        //get column-name, put to attr.name
        int32_t namelen;
        memcpy(&namelen, (char *) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char attrname[namelen + 1];
        attrname[namelen] ='\0';
        memcpy(attrname, (char *) data + offset, namelen);
        offset += namelen;
        attrp.attr.name = (string) attrname;

        //get colum-type, put to attr.type
        int32_t type;
        memcpy(&type, (char *) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attrp.attr.type = AttrType(type);

        //get column -length, put to attr.length
        int32_t length;
        memcpy(&length, (char *) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attrp.attr.length = AttrLength(length);

        // get position
        int32_t pos;
        memcpy(&pos, (char *) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attrp.pos = pos;

        attrP.push_back(attrp);
    }
    if (rc != RBFM_EOF) {
        return rc;
    }

    rc = rbfm->closeFile(fileHandle);
    if (rc)
        return rc;
    scanIterator.close();
    free(data);

    for (AttrwithPos attr : attrP) {
        attrs.push_back(attr.attr);
    }
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (tableName == "Tables" || "Columns") {
        return RM_CANNOT_DELETE_SYS;
    }

    FileHandle fileHandle;
    RC rc = rbfm->openFile(tableName + ".ext", fileHandle);
    if (rc) return rc;

    vector<Attribute> newtableDescriptor;
    rc = getAttributes(tableName, newtableDescriptor);
    if (rc) return rc;

    rc = rbfm->insertRecord(fileHandle, newtableDescriptor, data, rid);
    if (rc) return rc;

    rc = rbfm->closeFile(fileHandle);
    return rc;
}


//
RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (tableName == "Tables" || "Columns") {
        return RM_CANNOT_DELETE_SYS;
    }

    FileHandle fileHandle;
    RC rc = rbfm->openFile(tableName + ".ext", fileHandle);
    if (rc) return rc;

    vector<Attribute> newtableDescriptor;
    rc = getAttributes(tableName, newtableDescriptor);
    if (rc) return rc;

    rc = rbfm->deleteRecord(fileHandle, newtableDescriptor, rid);
    if (rc) return rc;

    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if (tableName == "Tables" || "Columns") {
        return RM_CANNOT_DELETE_SYS;
    }

    FileHandle fileHandle;
    RC rc = rbfm->openFile(tableName + ".ext", fileHandle);
    if (rc) return rc;

    vector<Attribute> newtableDescriptor;
    rc = getAttributes(tableName, newtableDescriptor);
    if (rc) return rc;

    rc = rbfm->updateRecord(fileHandle, newtableDescriptor, data, rid);
    if (rc) return rc;

    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    RC rc = rbfm->openFile(tableName + ".ext", fileHandle);
    if (rc) return rc;

    vector<Attribute> newtableDescriptor;
    rc = getAttributes(tableName, newtableDescriptor);
    if (rc) return rc;

    rc = rbfm->readRecord(fileHandle, newtableDescriptor, rid, data);
    if (rc) return rc;

    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    RC rc = rbfm->printRecord(attrs, data);
    if (rc) return rc;
    return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    FileHandle fileHandle;
    RC rc = rbfm->openFile(tableName + ".ext", fileHandle);
    if (rc) return rc;

    vector<Attribute> newtableDescriptor;
    rc = getAttributes(tableName, newtableDescriptor);
    if (rc) return rc;

    rc = rbfm->readAttribute(fileHandle, newtableDescriptor, rid, attributeName, data);
    if (rc) return rc;

    rc = rbfm->closeFile(fileHandle);
    return rc;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    return rbfm_scanIterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
    RecordBasedFileManager * rbfm = RecordBasedFileManager:: instance();
    rbfm_scanIterator.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}



