//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parallel_seq_scan_executor.h
//
// Identification: src/include/executor/parallel_seq_scan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "planner/seq_scan_plan.h"
#include "executor/abstract_scan_executor.h"
#include "executor/abstract_task.h"

namespace peloton {
namespace executor {

class ParallelSeqScanExecutor : public AbstractScanExecutor {
 public:
  ParallelSeqScanExecutor(const ParallelSeqScanExecutor &) = delete;
  ParallelSeqScanExecutor &operator=(const ParallelSeqScanExecutor &) = delete;
  ParallelSeqScanExecutor(ParallelSeqScanExecutor &&) = delete;
  ParallelSeqScanExecutor &operator=(ParallelSeqScanExecutor &&) = delete;

  explicit ParallelSeqScanExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /* iterator to traverse the list of tile groups embedded in the task */
  TileGroupPtrList::iterator tile_group_itr_;

  /* end iterator of the tile group list */
  TileGroupPtrList::const_iterator tile_group_end_itr_;

  /* ID of the task this executor runs */
  int task_id_;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;
};


}  // namespace executor
}  // namespace peloton