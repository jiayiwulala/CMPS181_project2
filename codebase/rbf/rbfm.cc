#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

// helper function

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
    );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
    );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
                break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
                break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
                break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page)
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;

        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                    break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                    break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                    break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

//start
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_NO_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);

    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
                break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
                break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
                break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    //find the page rid = page id, slot id
    //put to mem
    //delete record data, update slot directory header (offset, length = -1),
    //rearrange page, when a record is deleted, you need to move up all the records after it, so that there are no gaps
    //rid not change ,  the information in their slots changes
    // filehandler writing the page to disk write back
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(rid.pageNum, pageData)){
        free(pageData);
        return RBFM_READ_FAILED;
    }

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum){
        free(pageData);
        return RBFM_SLOT_NO_EXIST;
    }


    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.length == 0){
        free(pageData);
        return RBFM_DELETE_FAILED;
    }
    else if (recordEntry.offset <= 0) {
        RID newrid;
        newrid.pageNum = recordEntry.length;
        newrid.slotNum = -recordEntry.offset;

        if (deleteRecord(fileHandle, recordDescriptor, newrid)) {
            free(pageData);
            return RBFM_DELETE_FAILED;
        }
        recordEntry.length = 0;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
    }
    else {
        for (int i = rid.slotNum + 1; i < recordDescriptor.size(); i++) {
            // update the slot directory record entry data
            SlotDirectoryRecordEntry moveEntry = getSlotDirectoryRecordEntry(pageData, i);
            moveEntry.offset += recordEntry.length;
            setSlotDirectoryRecordEntry(pageData, i, moveEntry);
        }

        // Updating the slot directory header.
        slotHeader.freeSpaceOffset += recordEntry.length;
        slotHeader.recordEntriesNumber -= 1;
        setSlotDirectoryHeader(pageData, slotHeader);

        // set deleted recordentry length  = 0
        recordEntry.length = 0;
        setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
    }
    // Writing the page to disk.

    if (fileHandle.writePage(rid.pageNum, pageData)){
        return RBFM_WRITE_FAILED;
    }

    free(pageData);
    return SUCCESS;

}

// Assume the RID does not change after an update
//when a record is updated to have a different length, that can be treated as a delete
// (with compaction) plus an insert if the record fits in the free space now on the page.
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {

    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    if (fileHandle.readPage(rid.pageNum, pageData)){
        free(pageData);
        return RBFM_READ_FAILED;
    }


    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum) {
        free(pageData);
        return RBFM_SLOT_NO_EXIST;
    }

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    if (recordEntry.length == 0){
        free(pageData);
        return RBFM_DELETE_FAILED;
    }
    if (recordEntry.offset <= 0) {
        RID newrid;
        newrid.pageNum = recordEntry.length;
        newrid.slotNum = -recordEntry.offset;
        free(pageData);
        return updateRecord(fileHandle, recordDescriptor, data, newrid);
    }
    // if updated record length stays the same, update it and done
    unsigned newrecordSize = getRecordSize(recordDescriptor, data);
    if (newrecordSize == recordEntry.length) {
        // Adding the record data.
        setRecordAtOffset (pageData, recordEntry.offset, recordDescriptor, data);
    }
        // if updated record length become smaller, update it and compact the page
        // move other records.
    else if (newrecordSize < recordEntry.length) {
        setRecordAtOffset (pageData, recordEntry.offset, recordDescriptor, data);
        unsigned reducedlength = recordEntry.length - newrecordSize;
        recordEntry.length = newrecordSize;
        for (int i = rid.slotNum + 1; i < recordDescriptor.size(); i++) {
            // update the slot directory record entry data
            SlotDirectoryRecordEntry moveEntry = getSlotDirectoryRecordEntry(pageData, i);
            moveEntry.offset += reducedlength;
            setSlotDirectoryRecordEntry(pageData, i, moveEntry);
        }
        // Updating the slot directory header.
        slotHeader.freeSpaceOffset += reducedlength;
        setSlotDirectoryHeader(pageData, slotHeader);
    }
    // if record become bigger, check if the record fits in the free space now on the page
    // if yes, delete the record and insert the record
    else {
        if (deleteRecord(fileHandle, recordDescriptor, rid)) {
            free(pageData);
            return RBFM_UPDATE_FAILED;
        }
        if (getPageFreeSpaceSize(pageData) >= newrecordSize) {
            // Adding the new record reference in the slot directory.
            SlotDirectoryRecordEntry newRecordEntry;
            newRecordEntry.length = newrecordSize;
            newRecordEntry.offset = slotHeader.freeSpaceOffset - newrecordSize;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

            // Updating the slot directory header.
            slotHeader.freeSpaceOffset = newRecordEntry.offset;
            slotHeader.recordEntriesNumber += 1;
            setSlotDirectoryHeader(pageData, slotHeader);

            // Adding the record data.
            setRecordAtOffset(pageData, newRecordEntry.offset, recordDescriptor, data);
        }
        else{
            //if no the record must be migrated to a new page that has enough free space.
            // when a record migrates, you should leave a forwarding address behind pointing to t
            // he new location of the record.
            //forwarding address
            //migrated to a new page that has enough free space

            //find a page with enough space
            RID new_rid;
            RC rc = insertRecord(fileHandle, recordDescriptor, data, new_rid);
            if (rc) {
                free(pageData);
                return rc;
            }
            //moved
            recordEntry.length = new_rid.pageNum;
            recordEntry.offset = -new_rid.slotNum;
            setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
            }
    }
    // Writing the page to disk.

    if (fileHandle.writePage(rid.pageNum, pageData)){
        free(pageData);
        return RBFM_WRITE_FAILED;
    }

    free(pageData);
    return SUCCESS;
    }

//Given a record descriptor, read a specific attribute of a record identified by a given rid.
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
    Attribute attr;
    unsigned i;
    for (i = 0; i < recordDescriptor.size(); i++) {
        if (recordDescriptor[i].name == attributeName) {
            attr = recordDescriptor[i];
            break;
        }
    }
    if (i == recordDescriptor.size()) {
        return RBFM_READ_FAILED;
    }
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (pageData == NULL) {
        return RBFM_MALLOC_FAILED;
    }
    if (fileHandle.readPage(rid.pageNum, pageData)){
        free(pageData);
        return RBFM_READ_FAILED;
    }

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_NO_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    //deleted
    if (recordEntry.length == 0) {
        free(pageData);
        return RBFM_RECORD_DELETE;
    }
    //moved
    if (recordEntry.offset < 0) {
        RID newrid;
        newrid.pageNum = recordEntry.length;
        newrid.slotNum = recordEntry.offset;
        free(pageData);
        return readAttribute(fileHandle, recordDescriptor, newrid, attributeName, data);
    }

    readAttributeFromRecord(pageData, recordEntry.offset, i, attr.type, data);
    free(pageData);

/*
    void * recordData = malloc(recordEntry.length);
    if (readRecord(fileHandle, recordDescriptor, rid, recordData)) {
        return RBFM_READ_FAILED;
    }

    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    unsigned offset = sizeof(RecordLength) + nullIndicatorSize + sizeof(RecordLength) * (i-1);

    unsigned recoffset;
    memcpy (&recoffset, (char *)recordData + offset, sizeof(RecordLength));


    memcpy(data, (char *)recordData + recoffset, attr.length);
    free(recordData);
    */
    return SUCCESS;
}

RBFM_ScanIterator:: RBFM_ScanIterator() {

    currpage = 0;
    currslot = 0;
    totalpage = 0;
    totalslot = 0;
    rbfm = RecordBasedFileManager::instance();
}



// Scan returns an iterator to allow the caller to go through the results one by one.
//scan doesn't return a result set; it returns an iterator.
//iterator include the following information
// (current) currpage, currslot, totalpage, totalslot, pageData, attrIndex
//filehandle, recordDesciptor, conditionAttribute, compOp, value, attributeNames, skiplist
//scan doesn't actually read all of the tuples in the table.  Instead, it sets up an iterator that can read all of the tuples in the table using getNextTuple.


RC RBFM_ScanIterator::getCurrPage() {
    if (filehandle.readPage(currpage, pageData)) {
        return RBFM_READ_FAILED;
    }

    SlotDirectoryHeader header = rbfm->getSlotDirectoryHeader(pageData);
    totalslot = header.recordEntriesNumber;
    return SUCCESS;
}

RC RBFM_ScanIterator::getNextSlot() {

    if (currslot >= totalslot) {
        currslot = 0;
        currpage++;
        if (currpage >= totalpage) {
            return RBFM_EOF;
        }
        RC rc = getCurrPage();
        if (rc) {
            return rc;
        }
    }

    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currslot);
    if (recordEntry.length == 0 || recordEntry.offset <= 0 || !conditionmeet()) {
        currslot++;
        return getNextSlot();

    }
    return SUCCESS;
}

bool RBFM_ScanIterator::checkScanCondition(int recordInt, CompOp compOp, const void*  value) {
    uint32_t intval;
    memcpy(&intval, value, INT_SIZE);

    switch (compOp) {
        case EQ_OP: return recordInt == intval;
        case LT_OP: return recordInt < intval;
        case LE_OP: return recordInt <= intval;
        case GT_OP: return recordInt > intval;
        case GE_OP: return recordInt >= intval;
        case NE_OP: return recordInt != intval;
        case NO_OP: return true;
        default: return false;
    }
}

bool RBFM_ScanIterator::checkScanCondition(float recordFloat, CompOp compOp, const void*  value) {
    float floatval;
    memcpy(&floatval, value, REAL_SIZE);

    switch (compOp) {
        case EQ_OP: return recordFloat == floatval;
        case LT_OP: return recordFloat < floatval;
        case LE_OP: return recordFloat <= floatval;
        case GT_OP: return recordFloat > floatval;
        case GE_OP: return recordFloat >= floatval;
        case NE_OP: return recordFloat != floatval;
        case NO_OP: return true;
        default: return false;
    }
}

bool RBFM_ScanIterator::checkScanCondition(char* recordChar, CompOp compOp, const void*  value) {
    int32_t valuesize;
    memcpy(&valuesize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valuesize + 1];
    memcpy(valueStr, (char *) value + VARCHAR_LENGTH_SIZE, valuesize);
    valueStr[valuesize] = '\0';

    //<0	the first character that does not match has a lower value in ptr1 than in ptr2
    //0	the contents of both strings are equal
    //>0	the first character that does not match has a greater value in ptr1 than in ptr2
    int cmp = strcmp(recordChar, valueStr);
    switch (compOp) {
        case EQ_OP:
            return cmp == 0;
        case LT_OP:
            return cmp < 0;
        case LE_OP:
            return cmp <= 0;
        case GT_OP:
            return cmp > 0;
        case GE_OP:
            return cmp >= 0;
        case NE_OP:
            return cmp != 0;
        case NO_OP:
            return true;
        default:
            return false;
    }
}

void RecordBasedFileManager::readAttributeFromRecord(void *pageData, unsigned offset, unsigned attrIndex, AttrType type, void *data) {

    //get the attribute and put it in data
    char *start = (char *) pageData + offset;
    unsigned data_offset = 0;

    //get number of columns
    RecordLength n;
    memcpy(&n, start, sizeof(RecordLength));

    //get null indicator
    int recordNullIndicatorSize = getNullIndicatorSize(n);
    char recordNullIndicator[recordNullIndicatorSize];

    memcpy(recordNullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    char resultNullIndicator = 0;

    //if result is null, set null indicator for result

    if (fieldIsNull(recordNullIndicator, attrIndex)) {
        resultNullIndicator = 1;
    }
    memcpy(data, &resultNullIndicator, 1); // copy field null indicator to data
    data_offset += 1;
    if (resultNullIndicator) { return; }

    //if result is not null, put it to data
    // find attribute in page, put into data. data has one bit for null indicator, then attribute data.
    // if type is varchar, also need to copy varchar size
    unsigned attr_offset = sizeof(RecordLength) + recordNullIndicatorSize;
    // want to find attribute real length to copy, and start_offset to copy
    ColumnOffset start_offset;
    ColumnOffset end_offset;

    if (attrIndex > 0) {
        memcpy(&start_offset, start + attr_offset + (attrIndex - 1) * sizeof(ColumnOffset), sizeof(ColumnOffset));
    } else {
        start_offset = attr_offset + n * sizeof(ColumnOffset);
    }

    memcpy(&end_offset, start + attr_offset + attrIndex * sizeof(ColumnOffset), sizeof(ColumnOffset));

    unsigned attrlen = end_offset - start_offset;

    if (type == TypeVarChar) {
        memcpy((char *) data + data_offset, &attrlen, sizeof(VARCHAR_LENGTH_SIZE));
        data_offset += VARCHAR_LENGTH_SIZE;
    }

    memcpy((char *) data + data_offset, start + start_offset, attrlen);
}

bool RBFM_ScanIterator::conditionmeet(){
    if (compOp == NO_OP) {return true;}
    if (value == NULL)  {return false;}

    Attribute attr = recordDescriptor[attrIndex];

    //allocate memory for calculation
    void *data = malloc(1 + attr.length);
    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currslot);

    rbfm->readAttributeFromRecord(pageData, recordEntry.offset, attrIndex, attr.type, data);

    // now attribute information is in data
    char nullind;
    bool result = false;
    memcpy(&nullind, data, 1); // 1 is null, 0 is not  null

    if (nullind) {
        result =  false;
    }

    else if (attr.type == TypeInt) {
        int32_t recordInt;
        memcpy(&recordInt, (char *) data + 1, INT_SIZE);
        result = checkScanCondition(recordInt, compOp, value);
    }

    else if (attr.type == TypeReal) {
        float recordReal;
        memcpy(&recordReal, (char *) data + 1, REAL_SIZE);
        result = checkScanCondition(recordReal, compOp, value);
    }
    else if (attr.type == TypeVarChar) {
        uint32_t varCharlen;
        memcpy(&varCharlen, (char *) data + 1, VARCHAR_LENGTH_SIZE);
        char recordChar[varCharlen + 1];
        memcpy(recordChar, (char *) data + 1 + VARCHAR_LENGTH_SIZE, varCharlen);
        recordChar[varCharlen] = '\0';

        result = checkScanCondition(recordChar, compOp, value);

    }
    free(data);
    return result;

}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    RC rc = getNextSlot();
    if (rc) {
        return rc;
    }

    if (attributeNames.size() == 0) {
        rid.pageNum = currpage;
        rid.slotNum = currslot++;
        return SUCCESS;
    }

    //prepare null indicator
    unsigned nullIndicatorSize = rbfm->getNullIndicatorSize(attributeNames.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, currslot);

    //just show put into data, null indicator follow by field, varChar use 4 bytes to store the length of characters
    //only projected attribute
    void *buffer = malloc(PAGE_SIZE);
    if (buffer == NULL) {
        return RBFM_MALLOC_FAILED;
    }

    unsigned dataoffset = nullIndicatorSize;

    for (unsigned i = 0; i < attributeNames.size(); i++) {
        Attribute attr;
        unsigned j;
        for (j = 0; j < recordDescriptor.size(); j++) {
            if (recordDescriptor[j].name == attributeNames[i]) {
                attr = recordDescriptor[j];
                break;
            }
        }
        if (j == recordDescriptor.size()) {
            return RBFM_READ_FAILED;
        }
        type = attr.type;
        rbfm->readAttributeFromRecord(pageData, recordEntry.offset, j, type, buffer);

        char nullInd;
        memcpy(&nullInd, buffer, 1);
        if (nullInd) {
            int indicator_index = i / CHAR_BIT;
            char indicator_mask = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
            nullIndicator[indicator_index] |= indicator_mask;
        } else if (type == TypeInt) {
            memcpy((char *) data + dataoffset, (char *) buffer + 1, INT_SIZE);
            dataoffset += INT_SIZE;
        } else if (type == TypeReal) {
            memcpy((char *) data + dataoffset, (char *) buffer + 1, REAL_SIZE);
            dataoffset += REAL_SIZE;
        } else if (type == TypeVarChar) {
            uint32_t varcharSize;
            // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
            memcpy(&varcharSize, (char *) buffer + 1, VARCHAR_LENGTH_SIZE);
            memcpy((char *) data + dataoffset, &varcharSize, VARCHAR_LENGTH_SIZE);
            dataoffset += VARCHAR_LENGTH_SIZE;
            memcpy((char *) data + dataoffset, (char *) buffer + 1 + VARCHAR_LENGTH_SIZE, varcharSize);
            dataoffset += varcharSize;
        }
    }
    memcpy((char *) data, nullIndicator, nullIndicatorSize);
    free(buffer);
    rid.pageNum = currpage;
    rid.slotNum = currslot++;
    return SUCCESS;
}


RC RBFM_ScanIterator::close(){
    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute, // Specifically, the parameter conditionAttribute here is the attribute's name that you are going to apply the filter on
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator) {
    return rbfm_ScanIterator.scanInit(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}




RC RBFM_ScanIterator ::scanInit(FileHandle &fh,
                                const vector<Attribute> &recordD,
                                const string &conditionA, // Specifically, the parameter conditionAttribute here is the attribute's name that you are going to apply the filter on
                                const CompOp comp,                  // comparision type such as "<" and "="
                                const void *val,                    // used in the comparison
                                const vector<string> &attributes) {
    //scan starts from the first page first slot
    currpage = 0;
    currslot = 0;
    totalpage = 0;
    totalslot = 0;

    pageData = malloc(PAGE_SIZE);

    filehandle = fh;
    recordDescriptor = recordD;
    conditionAttribute = conditionA;
    compOp = comp;
    value = val;
    attributeNames = attributes;

    totalpage = filehandle.getNumberOfPages();
    if (totalpage > 0) {
        if(filehandle.readPage(0, pageData)) {
            return RBFM_READ_FAILED;
        }
        SlotDirectoryHeader slotDirectoryHeader = rbfm->getSlotDirectoryHeader(pageData);
        totalslot = slotDirectoryHeader.recordEntriesNumber;

        if (comp == NO_OP) {
            return SUCCESS;
        }

        Attribute attr;
        unsigned i;
        for (i = 0; i < recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == conditionAttribute) {
                attr = recordDescriptor[i];
                break;
            }
        }

        if (i == recordDescriptor.size()) {
            return RBFM_ScanIterator_ERROR;
        }
        attrIndex = i;
        type = attr.type;

        return SUCCESS;

    }
    else{
        return SUCCESS;
    }
}

