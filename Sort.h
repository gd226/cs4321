#ifndef __SORT__
#define __SORT__

#include "minirel.h"

#define    PAGESIZE    MINIBASE_PAGESIZE

class Sort
{
public:

	Sort(char		*inFile,			// Name of unsorted heapfile.

		 char		*outFile,		// Name of sorted heapfile.

		 int			numFields,		// Number of fields in input records.

		 AttrType	fieldTypes[],	// Array containing field types of input records.
		 // i.e. index of in[] ranges from 0 to (len_in - 1)

		 short		fieldSizes[],	// Array containing field sizes of input records.

		 int			sortKeyIndex,	// The number of the field to sort on.
		 // fld_no ranges from 0 to (len_in - 1).

		 TupleOrder	sortOrder,		// ASCENDING, DESCENDING

		 int			numBufPages,	// Number of buffer pages available for sorting.

		 Status     &s
		);

	~Sort() {}

private:
	Status PassZero(int &numTempFiles);

	Status PassOneAndBeyond(int numFiles);

	Status MergeManyToOne(unsigned int numSourceFiles, HeapFile **source, HeapFile *dest);

	Status OneMergePass(int numStartFiles, int numPass, int &numEndFiles);

	static char *CreateTempFilename(char *filename, int pass, int run);

private:
	int _recLength;
	int _numBufPages;
	char *_inFile;
	char *_outFile;
	short *_fieldSizes;
	int _sortKeyIndex;
};


#endif
