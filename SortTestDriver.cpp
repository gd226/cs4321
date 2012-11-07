#include <cmath>
#include <algorithm>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <vector>
using namespace std;

#include "heapfile.h"
#include "scan.h"

#include "Sort.h"
#include "SortTestDriver.h"

char *origKeys[] = {
	"raghu", "xbao", "cychan", "leela", "ketola", "soma", "ulloa", "dhanoa", "dsilva",
	"kurniawa", "dissoswa", "waic", "susanc", "kinc", "marc", "scottc", "yuc", "ireland",
	"rathgebe", "joyce", "daode", "yuvadee", "he", "huxtable", "muerle", "flechtne",
	"thiodore", "jhowe", "frankief", "yiching", "xiaoming", "jsong", "yung", "muthiah",
	"bloch", "binh", "dai", "hai", "handi", "shi", "sonthi", "evgueni", "chung-pi",
	"chui", "siddiqui", "mak", "tak", "sungk", "randal", "barthel", "newell", "schiesl",
	"neuman", "heitzman", "wan", "gunawan", "djensen", "juei-wen", "josephin", "harimin",
	"xin", "zmudzin", "feldmann", "joon", "wawrzon", "yi-chun", "wenchao", "seo",
	"karsono", "dwiyono", "ginther", "keeler", "peter", "lukas", "edwards", "mirwais",
	"schleis", "haris", "meyers", "azat", "shun-kit", "robert", "markert", "wlau",
	"honghu", "guangshu", "chingju", "bradw", "andyw", "gray", "vharvey", "awny",
	"savoy", "meltz"
};

char *sortedKeys[] = {
	"andyw", "awny", "azat", "barthel", "binh", "bloch", "bradw", "chingju",
	"chui", "chung-pi", "cychan", "dai", "daode", "dhanoa", "dissoswa", "djensen",
	"dsilva", "dwiyono", "edwards", "evgueni", "feldmann", "flechtne", "frankief",
	"ginther", "gray", "guangshu", "gunawan", "hai", "handi", "harimin", "haris",
	"he", "heitzman", "honghu", "huxtable", "ireland", "jhowe", "joon", "josephin",
	"joyce", "jsong", "juei-wen", "karsono", "keeler", "ketola", "kinc", "kurniawa",
	"leela", "lukas", "mak", "marc", "markert", "meltz", "meyers", "mirwais", "muerle",
	"muthiah", "neuman", "newell", "peter", "raghu", "randal", "rathgebe", "robert",
	"savoy", "schiesl", "schleis", "scottc", "seo", "shi", "shun-kit", "siddiqui",
	"soma", "sonthi", "sungk", "susanc", "tak", "thiodore", "ulloa", "vharvey", "waic",
	"wan", "wawrzon", "wenchao", "wlau", "xbao", "xiaoming", "xin", "yi-chun", "yiching",
	"yuc", "yung", "yuvadee", "zmudzin"
};

bool SortTestDriver::TestAll()
{
	bool succeed = true;

	succeed = TestSortOnly();
	//succeed = TestOneMerge();
	//succeed = TestMulMerge();
	//succeed = TestRandInt();

	return succeed;
}

bool SortTestDriver::TestSortOnly()
{
	int numRecords = 94;

	struct Record {
		char	key [32];
	} rec;

	AttrType 	attrType[] = { attrString };
	short		attrSize[] = { 32 };
	short		recLength  = 32;
	TupleOrder  sortOrder  = Ascending;

	// Create unsorted data file
	Status		s;
	RecordID	rid;

	HeapFile	f("SortOnly.in",s);

	assert(s == OK);

	for (int i = 0; i < numRecords; i++) {
		strcpy(rec.key, origKeys[i]);
		s = f.InsertRecord((char *)&rec, recLength, rid);
		assert(s == OK);
	}

	// Sort
	Sort sort("SortOnly.in", "SortOnly.out", 1, attrType, attrSize, 0, Ascending, 4, s);

	f.DeleteFile();

	if (s != OK) {
		cout << "Test SortOnly Failed: Sort function does not return OK" << endl;
		return false;
	}

	// Check intermediate results
	HeapFile notExist("SortOnly.out.sort.temp.1.0", s);

	if (s == OK && notExist.GetNumOfRecords() > 0) {
		cout << "Test SortOnly Warning: unexpected merge pass encountered." << endl;
	}

	// Check final result
	HeapFile f2("SortOnly.out", s);

	if (s != OK) {
		cout << "Test SortOnly Failed: output file cannot be found" << endl;
		return false;
	}

	Scan *scan = f2.OpenScan(s);
	assert(s == OK);

	int len = recLength;
	int count = 0;

	bool succeed = true;

	for (s = scan->GetNext(rid, (char *) &rec, len);
			(s == OK) && (count < numRecords);
			s = scan->GetNext(rid, (char *) &rec, len)) {
		if (strcmp(rec.key, sortedKeys[count]) != 0) {
			cout << "Test SortOnly Failed: output file is not sorted" << endl;
			succeed = false;
		}

		count++;
	}

	if (succeed && count != numRecords) {
		cout << "Test SortOnly Failed: sorted file does not have the same number of records as input file" << endl;
		succeed = false;
	}

	if (succeed) {
		cout << "Test SortOnly Succeeded" << endl;
	}

	f2.DeleteFile();

	return succeed;
}

bool SortTestDriver::TestOneMerge()
{
	int numRecords = 94;

	struct Record {
		char	key [32];
		char	pad [96];
	} rec;

	AttrType 	attrType[] = { attrString, attrString };
	short		attrSize[] = { 32, 96 };
	short		recLength  = 128;
	TupleOrder  sortOrder  = Ascending;

	// Create unsorted data file
	Status		s;
	RecordID	rid;

	HeapFile	f("OneMerge.in",s);

	assert(s == OK);

	for (int i = 0; i < numRecords; i++) {
		strcpy(rec.key, origKeys[i]);
		s = f.InsertRecord((char *)&rec, recLength, rid);
		assert(s == OK);
	}

	// Sort
	Sort sort("OneMerge.in", "OneMerge.out", 2, attrType, attrSize, 0, Ascending, 4, s);

	f.DeleteFile();

	if (s != OK) {
		cout << "Test OneMerge Failed: Sort function does not return OK" << endl;
		return false;
	}

	// Check intermediate results
	int totalNumRec = 0;
	HeapFile pass0file0("OneMerge.out.sort.temp.0.0", s);

	if (s != OK || pass0file0.GetNumOfRecords() == 0) {
		cout << "Test OneMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file0.GetNumOfRecords();
	}

	HeapFile pass0file1("OneMerge.out.sort.temp.0.1", s);

	if (s != OK || pass0file1.GetNumOfRecords() == 0) {
		cout << "Test OneMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file1.GetNumOfRecords();
	}

	HeapFile pass0file2("OneMerge.out.sort.temp.0.2", s);

	if (s != OK || pass0file2.GetNumOfRecords() == 0) {
		cout << "Test OneMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file2.GetNumOfRecords();
	}

	HeapFile pass0file3("OneMerge.out.sort.temp.0.3", s);

	if (s == OK && pass0file3.GetNumOfRecords() > 0) {
		cout << "Test OneMerge Warning: unexpected temp file encountered." << endl;
	}

	HeapFile notExist("OneMerge.out.sort.temp.2.0", s);

	if (s == OK && notExist.GetNumOfRecords() > 0) {
		cout << "Test OneMerge Warning: unexpected merge pass encountered." << endl;
	}

	if (totalNumRec != numRecords) {
		cout << "Test OneMerge Warning: incorect total number of records at the end of some pass." << endl;
	}

	// Check final result
	HeapFile f2("OneMerge.out", s);

	if (s != OK) {
		cout << "Test OneMerge Failed: output file cannot be found" << endl;
		return false;
	}

	Scan *scan = f2.OpenScan(s);
	assert(s == OK);

	int len = recLength;
	int count = 0;

	bool succeed = true;


	for (s = scan->GetNext(rid, (char *) &rec, len);
			(s == OK) && (count < numRecords);
			s = scan->GetNext(rid, (char *) &rec, len)) {
		if (strcmp(rec.key, sortedKeys[count]) != 0) {
			cout << "Test OneMerge Failed: output file is not sorted" << endl;
			succeed = false;
		}

		count++;
	}

	if (succeed && count != numRecords) {
		cout << "Test OneMerge Failed: sorted file does not have the same number of records as input file" << endl;
		succeed = false;
	}

	if (succeed) {
		cout << "Test OneMerge Succeeded" << endl;
	}

	f2.DeleteFile();

	return succeed;
}

bool SortTestDriver::TestMulMerge()
{
	int numRecords = 94;

	struct Record {
		char	key [32];
		char	pad [224];
	} rec;

	AttrType 	attrType[] = { attrString, attrString };
	short		attrSize[] = { 32, 224 };
	short		recLength  = 256;
	TupleOrder  sortOrder  = Ascending;

	// Create unsorted data file
	Status		s;
	RecordID	rid;

	HeapFile	f("MulMerge.in",s);

	assert(s == OK);

	for (int i = 0; i < numRecords; i++) {
		strcpy(rec.key, origKeys[i]);
		s = f.InsertRecord((char *)&rec, recLength, rid);
		assert(s == OK);
	}

	// Sort
	Sort sort("MulMerge.in", "MulMerge.out", 2, attrType, attrSize, 0, Ascending, 4, s);

	f.DeleteFile();

	if (s != OK) {
		cout << "Test MulMerge Failed: Sort function does not return OK" << endl;
		return false;
	}

	// Check intermediate results
	int totalNumRec = 0;
	HeapFile pass0file0("MulMerge.out.sort.temp.0.0", s);

	if (s != OK || pass0file0.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file0.GetNumOfRecords();
	}

	HeapFile pass0file1("MulMerge.out.sort.temp.0.1", s);

	if (s != OK || pass0file1.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file1.GetNumOfRecords();
	}

	HeapFile pass0file2("MulMerge.out.sort.temp.0.2", s);

	if (s != OK || pass0file2.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file2.GetNumOfRecords();
	}

	HeapFile pass0file3("MulMerge.out.sort.temp.0.3", s);

	if (s != OK || pass0file3.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file3.GetNumOfRecords();
	}

	HeapFile pass0file4("MulMerge.out.sort.temp.0.4", s);

	if (s != OK || pass0file4.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file4.GetNumOfRecords();
	}

	HeapFile pass0file5("MulMerge.out.sort.temp.0.5", s);

	if (s != OK || pass0file5.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass0file5.GetNumOfRecords();
	}

	HeapFile pass0file6("MulMerge.out.sort.temp.0.6", s);

	if (s == OK && pass0file6.GetNumOfRecords() > 0) {
		cout << "Test MulMerge Warning: unexpected temp file encountered." << endl;
	}

	if (totalNumRec != numRecords) {
		cout << "Test MulMerge Warning: incorect total number of records at the end of some pass." << endl;
	}

	totalNumRec = 0;
	HeapFile pass1file0("MulMerge.out.sort.temp.1.0", s);

	if (s != OK || pass1file0.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass1file0.GetNumOfRecords();
	}

	HeapFile pass1file1("MulMerge.out.sort.temp.1.1", s);

	if (s != OK || pass1file1.GetNumOfRecords() == 0) {
		cout << "Test MulMerge Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass1file1.GetNumOfRecords();
	}

	HeapFile pass1file2("MulMerge.out.sort.temp.1.2", s);

	if (s == OK && pass1file2.GetNumOfRecords() > 0) {
		cout << "Test MulMerge Warning: unexpected temp file encountered." << endl;
	}

	if (totalNumRec != numRecords) {
		cout << "Test MulMerge Warning: incorect total number of records at the end of some pass." << endl;
	}

	HeapFile notExist("MulMerge.out.sort.temp.3.0", s);

	if (s == OK && notExist.GetNumOfRecords() > 0) {
		cout << "Test MulMerge Warning: unexpected merge pass encountered." << endl;
	}


	// Check final result
	HeapFile f2("MulMerge.out", s);

	if (s != OK) {
		cout << "Test MulMerge Failed: output file cannot be found" << endl;
		return false;
	}

	Scan *scan = f2.OpenScan(s);
	assert(s == OK);

	int len = recLength;
	int count = 0;

	bool succeed = true;

	for (s = scan->GetNext(rid, (char *) &rec, len);
			(s == OK) && (count < numRecords);
			s = scan->GetNext(rid, (char *) &rec, len)) {
		if (strcmp(rec.key, sortedKeys[count]) != 0) {
			cout << "Test MulMerge Failed: output file is not sorted" << endl;
			succeed = false;
		}

		count++;
	}

	if (succeed && count != numRecords) {
		cout << "Test MulMerge Failed: sorted file does not have the same number of records as input file" << endl;
		succeed = false;
	}

	if (succeed) {
		cout << "Test MulMerge Succeeded" << endl;
	}

	f2.DeleteFile();

	return succeed;
}

bool SortTestDriver::TestRandInt()
{
	int numRecords = 5000;

	struct Record {
		char	pad [24];
		char	key [8];
	} rec;

	AttrType 	attrType[] = { attrString, attrInteger };
	short		attrSize[] = { 24, 8 };
	short		recLength  = 32;
	TupleOrder  sortOrder  = Ascending;

	// Create unsorted data file
	Status		s;
	RecordID	rid;

	HeapFile	f("RandInt.in", s);

	assert(s == OK);

	int orig[5000];

	for (int i = 0; i < numRecords; i++) {
		orig[i] = rand();
		char key[8];
		sprintf(key, "%d", orig[i]);

		strcpy(rec.key, key);

		s = f.InsertRecord((char *)&rec, recLength, rid);
		assert(s == OK);
	}

	// Sort
	Sort sort("RandInt.in", "RandInt.out", 2, attrType, attrSize, 1, Ascending, 6, s);

	f.DeleteFile();

	if (s != OK) {
		cout << "Test RandInt Failed: Sort function does not return OK" << endl;
		return false;
	}

	// Check intermediate results
	int totalNumRec = 0;
	HeapFile pass2file0("RandInt.out.sort.temp.2.0", s);

	if (s != OK || pass2file0.GetNumOfRecords() == 0) {
		cout << "Test RandInt Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass2file0.GetNumOfRecords();
	}

	HeapFile pass2file1("RandInt.out.sort.temp.2.1", s);

	if (s != OK || pass2file1.GetNumOfRecords() == 0) {
		cout << "Test RandInt Warning: expected temp file not exist." << endl;
	} else {
		totalNumRec += pass2file1.GetNumOfRecords();
	}

	HeapFile pass2file2("RandInt.out.sort.temp.2.2", s);

	if (s == OK && pass2file2.GetNumOfRecords() > 0) {
		cout << "Test RandInt Warning: unexpected temp file encountered." << endl;
	}

	if (totalNumRec != numRecords) {
		cout << "Test RandInt Warning: incorect total number of records at the end of some pass." << endl;
	}

	HeapFile notExist("RandInt.out.sort.temp.4.0", s);

	if (s == OK && notExist.GetNumOfRecords() > 0) {
		cout << "Test RandInt Warning: unexpected merge pass encountered." << endl;
	}

	// Check final result
	vector<int> sorted(orig, orig + sizeof(orig) / sizeof(int));
	std::sort(sorted.begin(), sorted.end());

	HeapFile f2("RandInt.out", s);

	if (s != OK) {
		cout << "Test RandInt Failed: output file cannot be found" << endl;
		return false;
	}

	Scan *scan = f2.OpenScan(s);
	assert(s == OK);

	int len = recLength;
	int count = 0;

	bool succeed = true;

	for (s = scan->GetNext(rid, (char *) &rec, len);
			(s == OK) && (count < numRecords);
			s = scan->GetNext(rid, (char *) &rec, len)) {
		int key = atoi(rec.key);

		if (key != sorted[count]) {
			cout << "Test RandInt Failed: output file is not sorted" << endl;
			succeed = false;
		}

		count++;
	}

	if (succeed && count != numRecords) {
		cout << "Test RandInt Failed: sorted file does not have the same number of records as input file" << endl;
		succeed = false;
	}

	if (succeed) {
		cout << "Test RandInt Succeeded" << endl;
	}

	f2.DeleteFile();

	return succeed;
}