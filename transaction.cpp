#include "transaction.h"
#include "lock.h"
#include <vector>
#include <algorithm>
using namespace std;

Transaction::Transaction()
{
	this->status = NEW;
}

Status Transaction::AbortTransaction()
{
	this->status = ABORTED;
	ReleaseAllLocks();
	cout << "Transaction " << this->tid << " Aborted" << endl;
	return OK;
}

void Transaction::InsertLock(int oid, bool shared)
{
	// TODO : Add your code here.
}

void Transaction::ReleaseAllLocks()
{
	// TODO : Add your code here.
}

Status Transaction::Read(KeyType key, DataType &value)
{
	// TODO : Add your code here.
	return OK;
}

Status Transaction::AddWritePair(KeyType key, DataType value, OpType op)
{
	// TODO : Add your code here.
	return OK;
}

Status Transaction::GroupWrite()
{
	// TODO : Add your code here.
	return OK;
}

Status Transaction::StartTranscation()
{
	if (this->status != NEW)
	{
		return FAIL;
	}

	this->status = RUNNING;

	cout << "Transaction " << this->tid << " Started" << endl;
	return OK;
}

Status Transaction::EndTransaction()
{
	if (this->status != ABORTED) {
		ReleaseAllLocks();
	}

	return OK;
}

Status Transaction::CommitTransaction()
{
	if ((this->status != RUNNING) && (this->status != GROUPWRITE))
	{
		return FAIL;
	}

	cout << "Transaction " << this->tid << " Committed" << endl;
	this->status = COMMITTED;
	return OK;
}
