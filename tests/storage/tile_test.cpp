/*-------------------------------------------------------------------------
*
* tile_test.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/test/tile_test.cpp
*
*-------------------------------------------------------------------------
*/

#include "gtest/gtest.h"

#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Tile Tests
//===--------------------------------------------------------------------===//

TEST(TileTests, BasicTest) {

	std::vector<catalog::Column> columns;

	catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "A", true);
	catalog::Column column2(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "B", true);
	catalog::Column column3(VALUE_TYPE_TINYINT, GetTypeSize(VALUE_TYPE_TINYINT), "C", true);
	catalog::Column column4(VALUE_TYPE_VARCHAR, 25, "D", false);

	columns.push_back(column1);
	columns.push_back(column2);
	columns.push_back(column3);
	columns.push_back(column4);

	catalog::Schema *schema = new catalog::Schema(columns);

  std::vector<std::string> column_names;

  column_names.push_back("COL 1");
  column_names.push_back("COL 2");
  column_names.push_back("COL 3");
  column_names.push_back("COL 4");

  const int tuple_count = 6;

  storage::AbstractBackend *backend = new storage::VMBackend();
  storage::TileGroupHeader *header = new storage::TileGroupHeader(backend, tuple_count);

  storage::Tile *tile = storage::TileFactory::GetTile(INVALID_OID, INVALID_OID, INVALID_OID, INVALID_OID,
                                                      header, backend, *schema, nullptr, tuple_count);

  storage::Tuple *tuple1 = new storage::Tuple(schema, true);
  storage::Tuple *tuple2 = new storage::Tuple(schema, true);

	tuple1->SetValue(0, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(1, ValueFactory::GetIntegerValue(1));
	tuple1->SetValue(2, ValueFactory::GetTinyIntValue(1));
	tuple1->SetValue(3, ValueFactory::GetStringValue("tuple 1", tile->GetPool()));

	tuple2->SetValue(0, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(1, ValueFactory::GetIntegerValue(2));
	tuple2->SetValue(2, ValueFactory::GetTinyIntValue(2));
	tuple2->SetValue(3, ValueFactory::GetStringValue("tuple 2", tile->GetPool()));


	tile->InsertTuple(0, tuple1);
	tile->InsertTuple(1, tuple2);
	tile->InsertTuple(2, tuple2);

	std::cout << (*tile);

	tile->InsertTuple(2, tuple1);

	std::cout << (*tile);

	delete tuple1;
	delete tuple2;
	delete tile;
	delete header;
	delete schema;
	delete backend;
}

} // End test namespace
} // End peloton namespace


