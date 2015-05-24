#pragma once

#include <string>
#include <sstream>

#include "common/value_factory.h"
#include "common/serializer.h"
#include "common/value_vector.h"
#include "expression/abstract_expression.h"

namespace nstore {
namespace expression {

//===--------------------------------------------------------------------===//
// Tuple Address Expression
//===--------------------------------------------------------------------===//

class TupleAddressExpression : public AbstractExpression {
 public:

  TupleAddressExpression()
 : AbstractExpression(EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS) {
  }

  inline Value Evaluate(const Tuple *tuple1, __attribute__((unused)) const Tuple *tuple2)  const {
    return ValueFactory::GetAddressValue(tuple1->GetData());
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "TupleAddressExpression\n";
  }
};

} // End expression namespace
} // End nstore namespace
