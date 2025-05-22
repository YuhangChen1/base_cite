/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <sstream>
#include <set>
#include <map>
#include "parser/ast.h"

#include "parser/parser.h"
// Assuming these are in common.h or a similar accessible header
// #include "common/common.h" 
// #include "defs.h"

typedef enum PlanTag {
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_ShowIndex,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_CreateIndex,
    T_DropIndex,
    T_SetKnob,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_NestLoop,
    T_SortMerge, // sort merge join
    T_Sort,
    T_Projection,
    T_Aggregate,
    T_Filter,
    T_Explain
} PlanTag;

// 查询执行计划
class Plan {
public:
    PlanTag tag;

    virtual ~Plan() = default;

    virtual std::string toString(int indent = 0) const { return ""; };
    virtual std::set<std::string> get_involved_tables() const { return {}; }
};

namespace { // Anonymous namespace for helper functions

std::string indent_string(int indent_level) {
    std::string res;
    for (int i = 0; i < indent_level; ++i) {
        res += "\t";
    }
    return res;
}

std::string tab_col_to_string(const TabCol& tc) {
    if (tc.tab_name.empty()) {
        return tc.col_name;
    }
    return tc.tab_name + "." + tc.col_name;
}

std::string value_to_string(const Value& val) {
    std::ostringstream oss;
    switch (val.type) {
        case TYPE_INT:
            oss << val.ival;
            break;
        case TYPE_FLOAT:
            oss << val.fval;
            break;
        case TYPE_STRING:
            oss << "'" << val.sval << "'";
            break;
        default:
            oss << "UNKNOWN_VALUE_TYPE";
            break;
    }
    return oss.str();
}

std::string comp_op_to_string(CompOp op) {
    switch (op) {
        case OP_EQ: return "=";
        case OP_NE: return "<>";
        case OP_LT: return "<";
        case OP_GT: return ">";
        case OP_LE: return "<=";
        case OP_GE: return ">=";
        default: return "UNKNOWN_OP";
    }
}

std::string condition_to_string(const Condition& cond) {
    std::ostringstream oss;
    oss << tab_col_to_string(cond.lhs_col);
    oss << " " << comp_op_to_string(cond.op) << " ";
    if (cond.is_rhs_val) {
        oss << value_to_string(cond.rhs_val);
    } else {
        oss << tab_col_to_string(cond.rhs_col);
    }
    return oss.str();
}

} // end anonymous namespace

class ScanPlan : public Plan {
public:
    ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
             std::vector<std::string> index_col_names) {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager->db_.get_table(tab_name_);
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;
        fed_conds_ = conds_;
        index_col_names_ = std::move(index_col_names);
    }

    ~ScanPlan() {
    }

    std::string toString(int indent = 0) const override {
        std::ostringstream oss;
        oss << indent_string(indent) << "Scan(table=" << tab_name_ << ")";
        return oss.str();
    }

    // 以下变量同ScanExecutor中的变量
    std::string tab_name_;
    std::vector<ColMeta> cols_;
    std::vector<Condition> conds_;
    size_t len_;
    std::vector<Condition> fed_conds_;
    std::vector<std::string> index_col_names_;

    std::set<std::string> get_involved_tables() const override { return {tab_name_}; }
};

class JoinPlan : public Plan {
public:
    JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds) {
        Plan::tag = tag;
        left_ = std::move(left);
        right_ = std::move(right);
        conds_ = std::move(conds);
        type = INNER_JOIN;
    }

    ~JoinPlan() {
    }

    void _collect_tables_recursive(std::set<std::string>& tables_set) const {
        // Delegate to the helper function in anonymous namespace for collecting tables from children
        if (left_) {
            collect_involved_tables_from_plan_node_recursive(left_, tables_set);
        }
        if (right_) {
            collect_involved_tables_from_plan_node_recursive(right_, tables_set);
        }
    }

    std::string toString(int indent = 0) const override {
        std::ostringstream oss;
        oss << indent_string(indent) << "Join(tables=[";
        
        std::set<std::string> tables_set;
        _collect_tables_recursive(tables_set);
        std::vector<std::string> tables_vec(tables_set.begin(), tables_set.end());
        std::sort(tables_vec.begin(), tables_vec.end());
        for (size_t i = 0; i < tables_vec.size(); ++i) {
            oss << tables_vec[i] << (i == tables_vec.size() - 1 ? "" : ",");
        }
        oss << "], condition=[";

        std::vector<std::string> cond_strs;
        for (const auto& cond : conds_) {
            cond_strs.push_back(condition_to_string(cond));
        }
        std::sort(cond_strs.begin(), cond_strs.end());
        for (size_t i = 0; i < cond_strs.size(); ++i) {
            oss << cond_strs[i] << (i == cond_strs.size() - 1 ? "" : ",");
        }
        oss << "])";
        if (left_) {
            oss << "\n" << left_->toString(indent + 1);
        }
        if (right_) {
            oss << "\n" << right_->toString(indent + 1);
        }
        return oss.str();
    }

    // 左节点
    std::shared_ptr<Plan> left_;
    // 右节点
    std::shared_ptr<Plan> right_;
    // 连接条件
    std::vector<Condition> conds_;
    // future TODO: 后续可以支持的连接类型
    JoinType type;

    std::set<std::string> get_involved_tables() const override {
        std::set<std::string> tables;
        if (left_) {
            auto left_tables = left_->get_involved_tables();
            tables.insert(left_tables.begin(), left_tables.end());
        }
        if (right_) {
            auto right_tables = right_->get_involved_tables();
            tables.insert(right_tables.begin(), right_tables.end());
        }
        return tables;
    }
};

class ProjectionPlan : public Plan {
public:
    ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols,
                   std::vector<std::string> alias) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_cols_ = std::move(sel_cols);
        alias_ = std::move(alias);
    }

    ~ProjectionPlan() {
    }

    std::string toString(int indent = 0) const override {
        std::ostringstream oss;
        oss << indent_string(indent) << "Project(columns=[";
        if (sel_cols_.empty() || (sel_cols_.size() == 1 && sel_cols_[0].col_name == "*") ) {
            oss << "*";
        } else {
            std::vector<std::string> col_strs;
            for (const auto& tc : sel_cols_) {
                col_strs.push_back(tab_col_to_string(tc));
            }
            std::sort(col_strs.begin(), col_strs.end());
            for (size_t i = 0; i < col_strs.size(); ++i) {
                oss << col_strs[i] << (i == col_strs.size() - 1 ? "" : ",");
            }
        }
        oss << "])";
        if (subplan_) {
            oss << "\n" << subplan_->toString(indent + 1);
        }
        return oss.str();
    }

    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    std::vector<std::string> alias_;

    std::set<std::string> get_involved_tables() const override {
        return subplan_ ? subplan_->get_involved_tables() : std::set<std::string>();
    }
};

class SortPlan : public Plan {
public:
    SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        sel_col_ = std::move(sel_col);
        is_desc_ = is_desc;
    }

    ~SortPlan() {
    }

    std::string toString(int indent = 0) const override {
        // Sort itself doesn't add to the plan string structure,
        // but its subplan does.
        if (subplan_) {
            return subplan_->toString(indent);
        }
        return "";
    }

    std::shared_ptr<Plan> subplan_;
    TabCol sel_col_;
    bool is_desc_;

    std::set<std::string> get_involved_tables() const override {
        return subplan_ ? subplan_->get_involved_tables() : std::set<std::string>();
    }
};

class AggregatePlan : public Plan {
public:
    AggregatePlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols,
                  std::vector<AggType> agg_types,
                  std::vector<TabCol> group_bys, std::vector<Condition> havings) : subplan_(std::move(subplan)),
        sel_cols_(std::move(sel_cols)), agg_types_(std::move(agg_types)),
        group_bys_(std::move(group_bys)), havings_(std::move(havings)) {
        Plan::tag = tag;
    }

    ~AggregatePlan() {
    }

     std::string toString(int indent = 0) const override {
        // Aggregate itself doesn't add to the plan string structure for now,
        // but its subplan does.
        // Future: Could add info about group by keys or aggregate functions.
        if (subplan_) {
            return subplan_->toString(indent);
        }
        return "";
    }

    std::shared_ptr<Plan> subplan_;
    std::vector<TabCol> sel_cols_;
    std::vector<AggType> agg_types_;

    std::vector<TabCol> group_bys_;
    std::vector<Condition> havings_;

    std::set<std::string> get_involved_tables() const override {
        return subplan_ ? subplan_->get_involved_tables() : std::set<std::string>();
    }
};

// Filter Plan
class FilterPlan : public Plan {
public:
    FilterPlan(std::shared_ptr<Plan> subplan, std::vector<Condition> conds)
        : subplan_(std::move(subplan)), conds_(std::move(conds)) {
        Plan::tag = T_Filter;
    }

    ~FilterPlan() {}

    std::string toString(int indent = 0) const override {
        std::ostringstream oss;
        oss << indent_string(indent) << "Filter(condition=[";
        std::vector<std::string> cond_strs;
        for (const auto& cond : conds_) {
            cond_strs.push_back(condition_to_string(cond));
        }
        std::sort(cond_strs.begin(), cond_strs.end());
        for (size_t i = 0; i < cond_strs.size(); ++i) {
            oss << cond_strs[i] << (i == cond_strs.size() - 1 ? "" : ",");
        }
        oss << "])";
        if (subplan_) {
            oss << "\n" << subplan_->toString(indent + 1);
        }
        return oss.str();
    }

    std::shared_ptr<Plan> subplan_;
    std::vector<Condition> conds_;

    std::set<std::string> get_involved_tables() const override {
        return subplan_ ? subplan_->get_involved_tables() : std::set<std::string>();
    }
};

// dml语句，包括insert; delete; update; select语句　
class DMLPlan : public Plan {
public:
    DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::string tab_name,
            std::vector<Value> values, std::vector<Condition> conds,
            std::vector<SetClause> set_clauses) {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
        tab_name_ = std::move(tab_name);
        values_ = std::move(values);
        conds_ = std::move(conds);
        set_clauses_ = std::move(set_clauses);
    }

    ~DMLPlan() {
    }

    std::string toString(int indent = 0) const override {
        if (Plan::tag == T_select && subplan_) {
            return subplan_->toString(indent);
        }
        return ""; // Other DML types don't have a plan string for EXPLAIN
    }

    std::shared_ptr<Plan> subplan_;
    std::string tab_name_;
    std::vector<Value> values_;
    std::vector<Condition> conds_;
    std::vector<SetClause> set_clauses_;

    std::set<std::string> get_involved_tables() const override {
        if (subplan_) { // This case is mainly for T_select
            return subplan_->get_involved_tables();
        }
        // For Insert, Update, Delete, if tab_name_ is considered "involved"
        // for the purpose of this method, then return {tab_name_}.
        // However, for join ordering, DML plans usually aren't part of the join tree directly.
        // If this DMLPlan wraps a SELECT (subplan_), its tables are relevant.
        // Otherwise, it might be an operation on a single table.
        if (!tab_name_.empty()) return {tab_name_};
        return {};
    }
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan {
public:
    DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols) {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
        cols_ = std::move(cols);
        tab_col_names_ = std::move(col_names);
    }

    ~DDLPlan() {
    }
    // DDL plans typically don't have a complex structure to print for EXPLAIN
    // std::string toString(int indent = 0) const override { return ""; }


    std::string tab_name_;
    std::vector<std::string> tab_col_names_;
    std::vector<ColDef> cols_;
    // DDL plans operate on a specific table, but don't have subplans for get_involved_tables
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan {
public:
    OtherPlan(PlanTag tag, std::string tab_name) {
        Plan::tag = tag;
        tab_name_ = std::move(tab_name);
    }

    ~OtherPlan() {
    }
    // Other plans typically don't have a complex structure to print for EXPLAIN
    // std::string toString(int indent = 0) const override { return ""; }

    std::string tab_name_;
    // Other plans operate on a specific table or are general, no subplans for get_involved_tables
};

// Set Knob Plan
class SetKnobPlan : public Plan {
public:
    SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
        Plan::tag = T_SetKnob;
        set_knob_type_ = knob_type;
        bool_value_ = bool_value;
    }

    ast::SetKnobType set_knob_type_;
    bool bool_value_;
    // SetKnob plans typically don't have a complex structure to print for EXPLAIN
    // std::string toString(int indent = 0) const override { return ""; }
};

// Explain Plan
class ExplainPlan : public Plan {
public:
    std::shared_ptr<Plan> explained_plan_;

    ExplainPlan(std::shared_ptr<Plan> p) : explained_plan_(std::move(p)) {
        Plan::tag = T_Explain;
    }

    ~ExplainPlan() {}

    std::string toString(int indent = 0) const override {
        return explained_plan_ ? explained_plan_->toString(indent) : "";
    }
    std::set<std::string> get_involved_tables() const override {
        return explained_plan_ ? explained_plan_->get_involved_tables() : std::set<std::string>();
    }
};

class plannerInfo {
public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan> > table_scan_executors;
    std::vector<SetClause> set_clauses;

    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_): parse(std::move(parse_)) {
    }

    // Helper method for JoinPlan to recursively collect table names
    // This needs to be available to JoinPlan's _collect_tables_recursive
    // For simplicity, it's outside, but ideally it would be a static member or friend
    // Or, better, each Plan type would have a virtual method to collect its tables.
    // void collect_involved_tables_recursive(const Plan* plan, std::set<std::string>& tables_set) {
    //     if (!plan) return;

    //     if (auto scan_plan = dynamic_cast<const ScanPlan*>(plan)) {
    //         tables_set.insert(scan_plan->tab_name_);
    //     } else if (auto join_plan = dynamic_cast<const JoinPlan*>(plan)) {
    //         collect_involved_tables_recursive(join_plan->left_.get(), tables_set);
    //         collect_involved_tables_recursive(join_plan->right_.get(), tables_set);
    //     } else if (auto projection_plan = dynamic_cast<const ProjectionPlan*>(plan)) {
    //         collect_involved_tables_recursive(projection_plan->subplan_.get(), tables_set);
    //     } else if (auto filter_plan = dynamic_cast<const FilterPlan*>(plan)) {
    //         collect_involved_tables_recursive(filter_plan->subplan_.get(), tables_set);
    //     } else if (auto sort_plan = dynamic_cast<const SortPlan*>(plan)) {
    //         collect_involved_tables_recursive(sort_plan->subplan_.get(), tables_set);
    //     } else if (auto aggregate_plan = dynamic_cast<const AggregatePlan*>(plan)) {
    //         collect_involved_tables_recursive(aggregate_plan->subplan_.get(), tables_set);
    //     }
    //     // DMLPlan with T_select could also have a subplan
    //     else if (auto dml_plan = dynamic_cast<const DMLPlan*>(plan)) {
    //         if(dml_plan->tag == T_select) {
    //             collect_involved_tables_recursive(dml_plan->subplan_.get(), tables_set);
    //         }
    //     }
    // }
};

// Definition of the recursive helper for collecting table names.
// This needs to be after all relevant Plan classes are defined if using dynamic_pointer_cast.
// Moving this into the anonymous namespace as it's closely tied to plan implementations.
namespace { // Re-opening anonymous namespace if it was closed, or defining it if first use here

void collect_involved_tables_from_plan_node_recursive(const std::shared_ptr<Plan>& plan, std::set<std::string>& tables_set) {
    if (!plan) {
        return;
    }

    // Try to cast to specific plan types to extract information or recurse
    if (auto scan_plan = std::dynamic_pointer_cast<const ScanPlan>(plan)) {
        tables_set.insert(scan_plan->tab_name_);
    } else if (auto join_plan = std::dynamic_pointer_cast<const JoinPlan>(plan)) {
        // For JoinPlan, recursively call this helper on its children.
        // This avoids direct calls to _collect_tables_recursive from outside JoinPlan
        // and keeps the logic centralized here.
        if (join_plan->left_) {
            collect_involved_tables_from_plan_node_recursive(join_plan->left_, tables_set);
        }
        if (join_plan->right_) {
            collect_involved_tables_from_plan_node_recursive(join_plan->right_, tables_set);
        }
    } else if (auto projection_plan = std::dynamic_pointer_cast<const ProjectionPlan>(plan)) {
        if (projection_plan->subplan_) {
            collect_involved_tables_from_plan_node_recursive(projection_plan->subplan_, tables_set);
        }
    } else if (auto filter_plan = std::dynamic_pointer_cast<const FilterPlan>(plan)) {
        if (filter_plan->subplan_) {
            collect_involved_tables_from_plan_node_recursive(filter_plan->subplan_, tables_set);
        }
    } else if (auto sort_plan = std::dynamic_pointer_cast<const SortPlan>(plan)) {
        if (sort_plan->subplan_) {
            collect_involved_tables_from_plan_node_recursive(sort_plan->subplan_, tables_set);
        }
    } else if (auto aggregate_plan = std::dynamic_pointer_cast<const AggregatePlan>(plan)) {
        if (aggregate_plan->subplan_) {
            collect_involved_tables_from_plan_node_recursive(aggregate_plan->subplan_, tables_set);
        }
    } else if (auto dml_plan = std::dynamic_pointer_cast<const DMLPlan>(plan)) {
        if (dml_plan->tag == T_select && dml_plan->subplan_) {
            collect_involved_tables_from_plan_node_recursive(dml_plan->subplan_, tables_set);
        }
    }
    // Other plan types (DDLPlan, OtherPlan, SetKnobPlan) do not contribute tables or subplans in this context.
}

} // end anonymous namespace

// The comments below this line were related to the previous state of table collection helpers.
// They are no longer accurate as `collect_involved_tables_from_plan_node_recursive`
// is now defined in the anonymous namespace and called correctly by JoinPlan.
// These comments can be removed for clarity.

