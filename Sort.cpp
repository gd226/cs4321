
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>

#include "heapfile.h"
#include "scan.h"

#include "Sort.h"

int recLenGlobal;
short *fieldSizesGlobal;
int sortKeyIndexGlobal;
TupleOrder sortOrderGlobal;
AttrType *fieldTypesGlobal;

//-------------------------------------------------------------------
// Sort::CreateTempFilename
//
// Input   : file_name,	The output file name of the sorting task.
//			 pass,		The number of passes (assuming the sort phase is pass 0).
//			 run,		The run number within the pass.
// Output  : None.
// Return  : The temporary file name
// Example : File 7 in pass 3 for output file FOO will be named FOO.sort.temp.3.7.
// Note    : It is your responsibility to destroy the return filename string.
//-------------------------------------------------------------------
char *Sort::CreateTempFilename(char *filename, int pass, int run)
{
	char *name = new char[strlen(filename) + 20];
	sprintf(name,"%s.sort.temp.%d.%d", filename, pass, run);
	return name;
}

Sort::Sort(
	char		*inFile,			// Name of unsorted heapfile.
	char		*outFile,		// Name of sorted heapfile.
	int      	numFields,		// Number of fields in input records.
	AttrType 	fieldTypes[],	// Array containing field types of input records.
	// i.e. index of in[] ranges from 0 to (len_in - 1)
	short    	fieldSizes[],	// Array containing field sizes of input records.
	int       	sortKeyIndex,	// The number of the field to sort on.
	// fld_no ranges from 0 to (len_in - 1).
	TupleOrder 	sortOrder,		// ASCENDING, DESCENDING
	int       	numBufPages,	// Number of buffer pages available for sorting.
	Status 	&s)
{
	_inFile = inFile;
	_outFile = outFile;
	_numBufPages = numBufPages;
	_fieldSizes = fieldSizes;
	_sortKeyIndex = sortKeyIndex;

	_recLength = 0;
	for (int i = 0; i < numFields; i++)
		_recLength += fieldSizes[i];

	recLenGlobal = _recLength;
	fieldSizesGlobal = fieldSizes;
	sortKeyIndexGlobal = sortKeyIndex;
	sortOrderGlobal = sortOrder;
	fieldTypesGlobal = fieldTypes;


	int numTempFiles = 0;

	PassZero(numTempFiles);
	PassOneAndBeyond(numTempFiles);

	// TODO: when to write to outFile?

	s = OK;
}

/*
 * Return 0 if a == b
 * If Ascending:
 *	returns -1 if a < b
 *	returns 1 if a > b
 * If Descending:
 *	return -1 if a > b
 *	return 1 if a < b
 */
int cmp(const void *a, const void *b) 
{
	char *record1 = (char *)a;
	char *record2 = (char *)b;

	int keyIndex = 0; 

	for (int i = 0; i < sortKeyIndexGlobal; i++) {
		keyIndex += fieldSizesGlobal[i];
	}

	AttrType keyType = fieldTypesGlobal[sortKeyIndexGlobal];
	int keyLen = fieldSizesGlobal[sortKeyIndexGlobal];

	char *key1 = new char[keyLen];
	char *key2 = new char[keyLen];

	memcpy(key1, record1 + keyIndex, keyLen);
	memcpy(key2, record2 + keyIndex, keyLen);

	int eq;
	// if string, just use strcmp
	if (keyType == attrString) {
		eq = strcmp(key1, key2);
	} else if (keyType == attrInteger) {
		int len1 = strlen(key1);
		int len2 = strlen(key2);
		
		// TODO: how are negative numbers represented??
		// Here I assume the first character is '-'...confirm with TA?
		// if both numbers are the same sign, compare lengths
		if ((key1[0] == '-' && key2[0] == '-') || (key1[0] != '-' && key2[0] != '-')) {
			if (len1 < len2)
				eq = -1;
			else if (len1 > len2)
				eq = 1;
			else 
				eq = strcmp(key1, key2);

			if (key1[0] == '-')
				eq = -eq;
		} else { 
			// if they are different signs, strcmp should always 
			// find the key beginning with '-' is smaller
			eq = strcmp(key1, key2);
		}
	}

	if (sortOrderGlobal == Descending)
		return -eq;

	return eq;
}

Status Sort::PassZero(int &numTempFiles) 
{
	Status status;
	HeapFile hf(_inFile, status);
	if (status != OK)
		return FAIL;
	Scan *scanner = hf.OpenScan(status);
	if (status != OK)
		return FAIL;

	// allocate memory
	int arrLen = PAGESIZE * _numBufPages;
	char *arr = new char[arrLen];
	int arrIndex, numRecs;
	numTempFiles = 0;

	char *recPtr = new char[_recLength];
	RecordID rid;

	int recLen = _recLength;
	bool finished = false;
	while (!finished) {

		numRecs = 0;
		arrIndex = 0;

		while (arrIndex < arrLen - _recLength) {
			status = scanner->GetNext(rid, recPtr, recLen);
			// is this right?? can we assume DONE means it's finished?
			if (status == DONE) {
				finished = true;
				break;
			} else if (status == FAIL) {
				goto exit;
			}

			memcpy(arr + arrIndex, recPtr, _recLength);
			numRecs++;
			arrIndex += _recLength;
		}

		delete scanner;

		qsort(arr, numRecs, _recLength, cmp);

		// TODO: WHAT TO NAME THE FILES?
		//const char *filename = CreateTempFilename("tmp", 0, numTempFiles++);
		const char *filename = _outFile;

		HeapFile tmpHf(filename, status);
		if (status != OK)
			goto exit;

		for (int i = 0; i < arrLen; i += _recLength) {
			if (tmpHf.InsertRecord(arr + i, _recLength, rid) != OK)
				goto exit;
		}
	}

exit:
	delete arr;
	return OK;
}

// heeellllppp T__T 
Status Sort::PassOneAndBeyond(int numFiles) 
{

	int pass = 0;
	int run = 0;
	char *filename = CreateTempFilename(_outFile, pass, run);

	return OK;
}


Status Sort::MergeManyToOne(unsigned int numSourceFiles, HeapFile **source, HeapFile *dest) 
{
	Status status;

	char **recPtrArr = new char*[numSourceFiles];
	Scan **scannerArr = new Scan*[numSourceFiles];
	int indexToAdvance = -1;

	RecordID rid;
	char *recPtr;
	int recLen = _recLength;

	for (int i = 0; i < numSourceFiles; i++) {
		HeapFile *hf = source[i];
		Scan *scanner = hf->OpenScan(status);
		if (status != OK)
			return FAIL;
		scannerArr[i] = scanner;

		scanner->GetNext(rid, recPtr, recLen);
		recPtrArr[i] = recPtr;
	}

	int numNull;
	while (1) {
		// next is either the current minimum or maximum element among all
		// the scans depending on sort order
		char *next = recPtrArr[0];
		indexToAdvance = 0;

		for (int i = 0, numNull = 0; i < numSourceFiles; i++) {
			recPtr = recPtrArr[i];
			if (recPtr == nullptr) {
				// numNull keeps track of which scans have reached the end
				numNull++;
				continue;
			}

			if (next == nullptr || cmp(next, recPtr) == 1) {
				next = recPtr;
				indexToAdvance = i;
			}
		}

		// we've reached the end of all scans; exit loop
		if (numNull == numSourceFiles)
			break;

		status = scannerArr[indexToAdvance]->GetNext(rid, recPtr, recLen);
		if (status == FAIL)
			return FAIL;
		if (status == DONE)
			recPtrArr[indexToAdvance] = nullptr;
		else 
			recPtrArr[indexToAdvance] = recPtr;
	}

	return OK;
}


// TODO: wtf does this do??
Status Sort::OneMergePass(int numStartFiles, int numPass, int &numEndFiles) 
{

	return OK;
}