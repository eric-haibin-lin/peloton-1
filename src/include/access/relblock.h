/*-------------------------------------------------------------------------
 *
 * relblock_io.h
 *	  POSTGRES relation block io utilities definitions.
 *
 *
 * src/include/access/relblock_io.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELBLOCK_IO_H
#define RELBLOCK_IO_H

#include "access/htup.h"
#include "utils/relcache.h"
#include "storage/buf.h"
#include "access/heapam.h"

#define BLOCK_FIXED_LENGTH_SIZE 100             /* In terms of number of tuples */
#define BLOCK_VARIABLE_LENGTH_SIZE 1024 * 16    /* Raw size in bytes */

#define BLOCK_POINTER_SIZE  8  /* 8 bytes */
#define NUM_REL_BLOCK_ENTRIES 1000

// RelationBlock storage information
typedef enum RelationBlockBackend{
    STORAGE_BACKEND_FS,
    STORAGE_BACKEND_VM,
    STORAGE_BACKEND_NVM
} RelationBlockBackend;

#define STORAGE_BACKEND_DEFAULT STORAGE_BACKEND_FS

/* Possible block types */
typedef enum RelationBlockType
{
	/* Used to store fixed-length tuples */
	RELATION_FIXED_BLOCK_TYPE,
	/* Used to store variable-length attributes */
	RELATION_VARIABLE_BLOCK_TYPE
} RelationBlockType;

/* RelationBlock structure */
typedef struct RelationBlockData
{
	RelationBlockType relblocktype;
	RelationBlockBackend relblockbackend;
	void *relblockdata;
	Size relblocklen;
} RelationBlockData;

typedef RelationBlockData* RelationBlock;

typedef struct RelationBlockInfoData
{
	Oid relid;

	/* relation blocks on VM */
	List* rel_fixed_blocks_on_VM;
	List* rel_variable_blocks_on_VM;

	/* relation blocks on NVM */
	List* rel_fixed_blocks_on_NVM;
	List* rel_variable_blocks_on_NVM;
} RelationBlockInfoData;

typedef RelationBlockInfoData* RelationBlockInfo;

/* HTAB */

/* Key for RelBlock Lookup Table */
typedef struct RelBlockTag{
	Oid       relid;
} RelBlockTag;

/* Entry for RelBlock Lookup Table */
typedef struct RelBlockLookupEnt{
	RelationBlockInfo relblockinfo;
} RelBlockLookupEnt;

extern HTAB *SharedRelBlockHash;

/* relblock.c */
extern void RelationInitBlockTableEntry(Relation relation);
extern void PrintAllRelationBlocks(Relation relation);
extern void RelationAllocateBlock(Relation relation, RelationBlockBackend relblockbackend,
								  RelationBlockType relblocktype);

/* relblock_table.c */
extern Size RelBlockTableShmemSize(int size);
extern void InitRelBlockTable(int size);
extern uint32 RelBlockTableHashCode(RelBlockTag *tagPtr);
extern RelBlockLookupEnt *RelBlockTableLookup(RelBlockTag *tagPtr, uint32 hashcode);
extern int	RelBlockTableInsert(RelBlockTag *tagPtr, uint32 hashcode, RelationBlockInfo relblockinfo);
extern void RelBlockTableDelete(RelBlockTag *tagPtr, uint32 hashcode);
extern void RelBlockTablePrint();

#endif   /* RELBLOCK_IO_H */
