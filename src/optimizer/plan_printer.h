#pragma once

#include <string>
#include <memory>
#include "optimizer/plan.h" // Assuming plan.h is the correct path
#include "common/common.h" // For Condition, TabCol etc. if needed directly

class PlanPrinter {
public:
    static std::string print(const std::shared_ptr<Plan>& plan);

private:
    static void print_node(const std::shared_ptr<Plan>& plan, std::string& output, int indent_level);
    static std::string format_condition_to_string(const Condition& cond);
    static std::string format_op(CompOp op);
    static std::string value_to_string(const Value& val);
    static void collect_join_tables(const std::shared_ptr<Plan>& plan, std::set<std::string>& table_names_set);
};
