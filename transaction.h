#ifndef _TRANSACTION_H
#define _TRANSACTION_H

#include<vector>
#include "da_types.h"
#include "new_error.h"
#include "HashIndex.h"
#include "threadsafe_HashIndex.h"

using namespace System;
using namespace System::Collections::Generic;

class ThreadSafeHashIndex;

#define PRINTTLOG 0

typedef struct KeyValuePair {
	KeyType key;
	DataType value;
	OpType op;
} KVP;

public class Transaction
{
public:
	// The status of the transaction. One of {NEW, RUNNING, GroupWrite, COMMITTED, ABORTED}
	TransactionStatus status;
	
	// The identifier for the transaction.
	TransactionID tid;

	// A handle to the concurrent hash index.
	ThreadSafeHashIndex *TSHI;

	// A list of all writes to be applied in the write phase of the transaction using GroupWrite.
	vector<KVP> writeList;

	// A list of all locks and the lock type currently held by the transaciton.
	vector< pair <int ,bool > > LockList;

	// Sets transaction status to NEW.
	Transaction();

	//--------------------------------------------------------------------
	// Transaction::Read
	//
	// Input    : The key whose value is to be read.
	// Output   : The value corresponding to the key.
	// Purpose  : Get the value of the key using the Hash Index with 
	//            appropriate locking. Abort Transaction and FAIL if locking
	//			  fails.
	// Return   : OK on success, FAIL on failure.
	//--------------------------------------------------------------------
	Status Read(KeyType key, DataType &value);

	//--------------------------------------------------------------------
	// Transaction::AddWritePair
	//
	// Input    : The key-value pair to be written with the type of the
	//			  operation (INSERT/UPDATE/DELETE)
	// Output   : None
	// Purpose  : Add a key-value pair to the writeList.
	// Return   : OK on success, FAIL on failure.
	//--------------------------------------------------------------------
	Status AddWritePair(KeyType key, DataType value, OpType op);


	//--------------------------------------------------------------------
	// Transaction::GroupWrite
	//
	// Input    : None. (it uses the writeList)
	// Output   : None
	// Purpose  : Performs all operations in writeList after acquiring 
	//			  appropriate locks. Abort transaction and FAIL is locking
	//            FAILS. NOTE : Order the lock-acquisition properly to avoid
	//            deadlocks. If all transactions follow the same order, then 
	//            there can be no deadlock unless due to lock upgrades (think why).
	// Return   : OK on success, FAIL on failure.
	//--------------------------------------------------------------------
	Status GroupWrite();

	// Checks current state, and Sets state to RUNNING.
	Status StartTranscation();

	//--------------------------------------------------------------------
	// Transaction::EndTransaction
	//
	// Input    : None
	// Purpose  : Release all locks Acquired by this transaction.
	// Return   : OK. Should never return FAIL.
	//--------------------------------------------------------------------
	Status EndTransaction();
	
	//--------------------------------------------------------------------
	// Transaction::CommitTransaction
	//
	// Input    : None
	// Purpose  : Change transaction status to COMMITTED.
	// Return   : OK. Should never return FAIL.
	//--------------------------------------------------------------------
	Status CommitTransaction();

	//--------------------------------------------------------------------
	// Transaction::AbortTransaction
	//
	// Input    : None
	// Purpose  : Change transaction status to ABORTED and Release all locks
	//			  Acquired by this transaction.
	// Return   : OK. Should never return FAIL.
	//--------------------------------------------------------------------
	Status AbortTransaction();

private:
	//--------------------------------------------------------------------
	// Transaction::ReleaseAllLocks
	//
	// Input    : None
	// Purpose  : Release all locks Acquired by the current transaction.
	//--------------------------------------------------------------------
	void	ReleaseAllLocks();

	//--------------------------------------------------------------------
	// Transaction::InsertLock
	//
	// Input    : The identifier or the object which is locked and if it is a shared lock.
	// Purpose  : Add entry into lock list to keep track of all locks Acquired by 
	//			  this transaction.
	// Return   : OK. Should never return FAIL.
	//--------------------------------------------------------------------
	void InsertLock(int oid, bool isShared);
};

public ref class TranscationExecution
{
public:
	Transaction *T;

	TranscationExecution()
	{
		T = new Transaction();
	}

	void run() {}
};
#endif