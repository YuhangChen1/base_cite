/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "planner.h"

#include <memory>
#include <set> // Required for std::set
#include <algorithm> // Required for std::remove_if, std::all_of

#include "execution/executor_delete.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

// 目前的索引匹配规则为：完全匹配索引字段，且全部为单点查询，不会自动调整where条件的顺序
// 支持自动调整where顺序，最长前缀匹配
bool Planner::get_index_cols(std::string &tab_name, std::vector<Condition> &curr_conds,
                             std::vector<std::string> &index_col_names) {
    // index_col_names.clear();
    // for (auto &cond: curr_conds) {
    //     if (cond.is_rhs_val && cond.op == OP_EQ && cond.lhs_col.tab_name == tab_name)
    //         index_col_names.push_back(cond.lhs_col.col_name);
    // }

    if (curr_conds.empty()) {
        return false;
    }

    // TODO
    TabMeta &tab = sm_manager_->db_.get_table(tab_name);
    // TODO 优化：减少索引文件名长度，提高匹配效率
    // conds重复去重

    std::set<std::string> index_set; // 快速查找
    std::unordered_map<std::string, int> conds_map; // 列名 -> Cond
    std::unordered_map<std::string, int> repelicate_conds_map;
    for (std::size_t i = 0; i < curr_conds.size(); ++i) {
        auto &col_name = curr_conds[i].lhs_col.col_name;
        if (index_set.count(col_name) == 0) {
            index_set.emplace(col_name);
            conds_map.emplace(col_name, i);
        } else {
            repelicate_conds_map.emplace(col_name, i);
        }
    }

    int max_len = 0, max_equals = 0, cur_len = 0, cur_equals = 0;
    for (auto &[index_name, index]: tab.indexes) {
        cur_len = cur_equals = 0;
        auto &cols = index.cols;
        for (auto &col: index.cols) {
            if (index_set.count(col.name) == 0) {
                break;
            }
            if (curr_conds[conds_map[col.name]].op == OP_EQ) {
                ++cur_equals;
            }
            ++cur_len;
        }
        // 如果有 where a = 1, b = 1, c > 1;
        // index(a, b, c), index(a, b, c, d);
        // 应该匹配最合适的，避免索引查询中带来的额外拷贝开销
        if (cur_len > max_len && cur_len < curr_conds.size()) {
            // 匹配最长的
            max_len = cur_len;
            index_col_names.clear();
            for (int i = 0; i < index.cols.size(); ++i) {
                index_col_names.emplace_back(index.cols[i].name);
            }
        } else if (cur_len == curr_conds.size()) {
            max_len = cur_len;
            // 最长前缀相等选择等号多的
            if (index_col_names.empty()) {
                for (int i = 0; i < index.cols.size(); ++i) {
                    index_col_names.emplace_back(index.cols[i].name);
                }
                // for (int i = 0; i < cur_len; ++i) {
                //     index_col_names.emplace_back(index.cols[i].name);
                // }
                // = = >  等号优先   = > =     = =
                // = > =           = = > _   = = _
                // } else if(index_col_names.size() > index.cols.size()) {
                //     // 选择最合适的，正好满足，这样减少索引查找的 memcpy
                //     index_col_names.clear();
                //     for (int i = 0; i < index.cols.size(); ++i) {
                //         index_col_names.emplace_back(index.cols[i].name);
                //     }
                // = = >  等号优先   = > =     = =
                // = > =           = = > _   = = _
                // 谁等号多选谁，不管是否合适
            } else if (cur_equals > max_equals) {
                max_equals = cur_equals;
                // cur_len >= cur_equals;
                index_col_names.clear();
                for (int i = 0; i < index.cols.size(); ++i) {
                    index_col_names.emplace_back(index.cols[i].name);
                }
                // for (int i = 0; i < cur_len; ++i) {
                //     index_col_names.emplace_back(index.cols[i].name);
                // }
            }
        }
    }

    // 没有索引
    if (index_col_names.empty()) {
        return false;
    }

    std::vector<Condition> fed_conds; // 理想谓词

    // 连接剩下的非索引列
    // 先清除已经在set中的
    for (auto &index_name: index_col_names) {
        if (index_set.count(index_name)) {
            index_set.erase(index_name);
            fed_conds.emplace_back(std::move(curr_conds[conds_map[index_name]]));
        }
    }

    // 连接 set 中剩下的
    for (auto &index_name: index_set) {
        fed_conds.emplace_back(std::move(curr_conds[conds_map[index_name]]));
    }

    // 连接重复的，如果有
    for (auto &[index_name, idx]: repelicate_conds_map) {
        fed_conds.emplace_back(std::move(curr_conds[repelicate_conds_map[index_name]]));
    }

    curr_conds = std::move(fed_conds);

    // 检查正确与否
    for (auto &index_name: index_col_names) {
        std::cout << index_name << ",";
    }
    std::cout << "\n";

    // if (tab.is_index(index_col_names)) return true;
    return true;
}

/**
 * @brief 表算子条件谓词生成
 *
 * @param conds 条件
 * @param tab_names 表名
 * @return std::vector<Condition>
 */
std::vector<Condition> pop_conds(std::vector<Condition> &conds, std::string tab_names) {
    // auto has_tab = [&](const std::string &tab_name) {
    //     return std::find(tab_names.begin(), tab_names.end(), tab_name) != tab_names.end();
    // };
    std::vector<Condition> solved_conds;
    auto it = conds.begin();
    while (it != conds.end()) {
        if ((tab_names.compare(it->lhs_col.tab_name) == 0 && it->is_rhs_val) || (
                it->lhs_col.tab_name.compare(it->rhs_col.tab_name) == 0)) {
            solved_conds.emplace_back(std::move(*it));
            it = conds.erase(it);
        } else {
            it++;
        }
    }
    return solved_conds;
}

int push_conds(Condition *cond, std::shared_ptr<Plan> plan) {
    if (auto x = std::dynamic_pointer_cast<ScanPlan>(plan)) {
        if (x->tab_name_.compare(cond->lhs_col.tab_name) == 0) {
            return 1;
        } else if (x->tab_name_.compare(cond->rhs_col.tab_name) == 0) {
            return 2;
        } else {
            return 0;
        }
    } else if (auto x = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        int left_res = push_conds(cond, x->left_);
        // 条件已经下推到左子节点
        if (left_res == 3) {
            return 3;
        }
        int right_res = push_conds(cond, x->right_);
        // 条件已经下推到右子节点
        if (right_res == 3) {
            return 3;
        }
        // 左子节点或右子节点有一个没有匹配到条件的列
        if (left_res == 0 || right_res == 0) {
            return left_res + right_res;
        }
        // 左子节点匹配到条件的右边
        if (left_res == 2) {
            // 需要将左右两边的条件变换位置
            std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
            };
            std::swap(cond->lhs_col, cond->rhs_col);
            cond->op = swap_op.at(cond->op);
        }
        x->conds_.emplace_back(std::move(*cond));
        return 3;
    }
    return false;
}

std::shared_ptr<Plan> Planner::pop_scan(int *scantbl, const std::string &table, std::vector<std::string> &joined_tables,
                                        std::vector<std::shared_ptr<Plan> > plans) {
    for (size_t i = 0; i < plans.size(); i++) {
        auto x = std::dynamic_pointer_cast<ScanPlan>(plans[i]);
        if (x->tab_name_ == table) {
            scantbl[i] = 1;
            joined_tables.emplace_back(x->tab_name_);
            return plans[i];
        }
    }
    return nullptr;
}

std::shared_ptr<Query> Planner::logical_optimization(std::shared_ptr<Query> query, Context *context) {
    //TODO 实现逻辑优化规则

    return query;
}


// --- Helper methods for Predicate Pushdown ---
std::shared_ptr<Plan> Planner::add_filter_if_needed(std::shared_ptr<Plan> plan, const std::vector<Condition>& conditions) {
    if (conditions.empty()) {
        return plan;
    }
    return std::make_shared<FilterPlan>(std::move(plan), conditions);
}

// Helper method to get all unique table names from a plan tree
std::set<std::string> Planner::get_plan_tables(const std::shared_ptr<Plan>& plan) {
    std::set<std::string> tables;
    if (!plan) return tables;

    switch (plan->tag) {
        case T_SeqScan:
        case T_IndexScan: {
            auto p = std::dynamic_pointer_cast<ScanPlan>(plan);
            tables.insert(p->tab_name_);
            break;
        }
        case T_Filter: {
            auto p = std::dynamic_pointer_cast<FilterPlan>(plan);
            tables = get_plan_tables(p->child_);
            break;
        }
        case T_Projection: {
            auto p = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            tables = get_plan_tables(p->subplan_);
            break;
        }
        case T_Sort: {
            auto p = std::dynamic_pointer_cast<SortPlan>(plan);
            tables = get_plan_tables(p->subplan_);
            break;
        }
        case T_Aggregate: {
            auto p = std::dynamic_pointer_cast<AggregatePlan>(plan);
            tables = get_plan_tables(p->subplan_);
            break;
        }
        case T_JoinPlan: // Generic JoinPlan if it exists, or handle specific join types
        case T_NestLoop:
        case T_SortMerge: {
            auto p = std::dynamic_pointer_cast<JoinPlan>(plan);
            auto left_tables = get_plan_tables(p->left_);
            auto right_tables = get_plan_tables(p->right_);
            tables.insert(left_tables.begin(), left_tables.end());
            tables.insert(right_tables.begin(), right_tables.end());
            break;
        }
        // DML plans might wrap other plans, but for predicate pushdown,
        // we are typically interested in the tables within the query part (subplan of DML)
        case T_Insert:
        case T_Update:
        case T_Delete:
        case T_select: { // T_select is a DMLPlan wrapper in this codebase
             auto p = std::dynamic_pointer_cast<DMLPlan>(plan);
             if(p && p->subplan_) { // Check if DMLPlan has a subplan to extract tables from
                 tables = get_plan_tables(p->subplan_);
             }
             // For Insert, Update, Delete, the target table (p->tab_name_) might also be relevant
             // depending on how conditions are evaluated. But for typical select pushdown,
             // subplan tables are key.
             break;
        }
        default:
            // Other plans (DDL, OtherPlan) might not have associated tables in this context
            break;
    }
    return tables;
}

// Helper method to check if a condition only uses columns from a given set of tables
bool Planner::condition_uses_only_tables(const Condition& cond, const std::set<std::string>& tables) {
    bool lhs_uses_tables = tables.count(cond.lhs_col.tab_name);
    bool rhs_uses_tables = true; // True if RHS is a value
    if (!cond.is_rhs_val) {
        rhs_uses_tables = tables.count(cond.rhs_col.tab_name);
    }
    return lhs_uses_tables && rhs_uses_tables;
}


std::shared_ptr<Plan> Planner::push_down_filters(std::shared_ptr<Plan> plan) {
    if (!plan) return nullptr;

    // Recurse first to push filters down in children
    if (auto join_plan = std::dynamic_pointer_cast<JoinPlan>(plan)) {
        join_plan->left_ = push_down_filters(join_plan->left_);
        join_plan->right_ = push_down_filters(join_plan->right_);
    } else if (auto proj_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan)) {
        proj_plan->subplan_ = push_down_filters(proj_plan->subplan_);
    } else if (auto filter_plan = std::dynamic_pointer_cast<FilterPlan>(plan)) {
         filter_plan->child_ = push_down_filters(filter_plan->child_);
    } else if (auto sort_plan = std::dynamic_pointer_cast<SortPlan>(plan)) {
        sort_plan->subplan_ = push_down_filters(sort_plan->subplan_);
    } else if (auto agg_plan = std::dynamic_pointer_cast<AggregatePlan>(plan)) {
        agg_plan->subplan_ = push_down_filters(agg_plan->subplan_);
    }
    // For DMLPlan, push filters into its subplan if it exists
    else if (auto dml_plan = std::dynamic_pointer_cast<DMLPlan>(plan)) {
        if (dml_plan->subplan_) {
            dml_plan->subplan_ = push_down_filters(dml_plan->subplan_);
        }
        // No filters to push down from DMLPlan itself usually, conditions are part of its definition or subplan
        return dml_plan;
    }


    // Actual pushdown logic for FilterNode
    if (auto filter_plan = std::dynamic_pointer_cast<FilterPlan>(plan)) {
        std::vector<Condition> remaining_conditions; // Conditions that cannot be pushed down further
        std::shared_ptr<Plan> current_child = filter_plan->child_;

        for (const auto& cond : filter_plan->conditions_) {
            if (auto join_child = std::dynamic_pointer_cast<JoinPlan>(current_child)) {
                std::set<std::string> left_tables = get_plan_tables(join_child->left_);
                std::set<std::string> right_tables = get_plan_tables(join_child->right_);

                if (condition_uses_only_tables(cond, left_tables)) {
                    join_child->left_ = add_filter_if_needed(join_child->left_, {cond});
                    join_child->left_ = push_down_filters(join_child->left_); // Push further down the left
                } else if (condition_uses_only_tables(cond, right_tables)) {
                    join_child->right_ = add_filter_if_needed(join_child->right_, {cond});
                    join_child->right_ = push_down_filters(join_child->right_); // Push further down the right
                } else {
                    // Condition uses columns from both sides or external, keep it at join level
                    // If it can be applied at this join, add to join_child->conds_
                    // For now, assume conditions not specific to one side stay above or become join conditions
                    // This part might need refinement based on how join conditions are handled vs filter conditions
                     remaining_conditions.push_back(cond); // Keep it for a filter above the join if not a join cond
                }
            } else if (auto proj_child = std::dynamic_pointer_cast<ProjectionPlan>(current_child)) {
                // Try to push filter below projection. This is valid if filter columns are available pre-projection.
                // For simplicity, we assume all columns needed by filters are available.
                // A more complex check would verify column availability.
                proj_child->subplan_ = add_filter_if_needed(proj_child->subplan_, {cond});
                // After adding, we need to re-run push_down_filters on the modified subplan of projection
                // to ensure the newly added filter is pushed as far down as possible.
                proj_child->subplan_ = push_down_filters(proj_child->subplan_); 
                // The filter is pushed, so it's not remaining at this level.
            }
             else {
                remaining_conditions.push_back(cond); // Cannot push further or no specific rule
            }
        }

        // If all conditions were pushed down, the FilterNode might be redundant
        if (remaining_conditions.empty()) {
            return current_child; // The child plan, potentially modified with filters pushed into it
        } else {
            // Some conditions remain, update the current FilterNode
            filter_plan->conditions_ = remaining_conditions;
            // The child (current_child) might have been modified (e.g. filters pushed into its children)
            // but the filter_plan still wraps current_child.
            filter_plan->child_ = current_child; 
            return filter_plan;
        }
    }

    return plan; // Return the (potentially modified) plan
}

// --- Helper methods for Projection Pushdown ---

std::vector<TabCol> Planner::convert_unordered_set_to_vector(const std::unordered_set<TabCol, TabColHash, TabColEqual>& cols_set) {
    std::vector<TabCol> vec(cols_set.begin(), cols_set.end());
    // Sorting for deterministic plan output, important for testing/debugging and EXPLAIN format
    std::sort(vec.begin(), vec.end(), [](const TabCol& a, const TabCol& b) {
        if (a.tab_name != b.tab_name) {
            return a.tab_name < b.tab_name;
        }
        return a.col_name < b.col_name;
    });
    return vec;
}

bool Planner::column_is_from_tables(const TabCol& tc, const std::set<std::string>& tables) {
    if (tc.tab_name.empty()) {
        // If table name is empty, it cannot be definitively associated with a specific set of named tables.
        // This might occur for literal expressions or globally unique column names if not fully qualified earlier.
        return false; 
    }
    return tables.count(tc.tab_name) > 0;
}

std::unordered_set<TabCol, TabColHash, TabColEqual> Planner::get_cols_required_by_node_itself(const std::shared_ptr<Plan>& plan) {
    std::unordered_set<TabCol, TabColHash, TabColEqual> required_cols;
    if (!plan) return required_cols;

    switch (plan->tag) {
        case T_Filter: {
            auto p = std::static_pointer_cast<FilterPlan>(plan);
            for (const auto& cond : p->conditions_) {
                if (!cond.lhs_col.col_name.empty()) required_cols.insert(cond.lhs_col);
                if (!cond.is_rhs_val && !cond.rhs_col.col_name.empty()) required_cols.insert(cond.rhs_col);
            }
            break;
        }
        case T_NestLoop:
        case T_SortMerge: { // Assuming JoinPlan is used for these tags
            auto p = std::static_pointer_cast<JoinPlan>(plan);
            for (const auto& cond : p->conds_) { // Join conditions
                if (!cond.lhs_col.col_name.empty()) required_cols.insert(cond.lhs_col);
                if (!cond.is_rhs_val && !cond.rhs_col.col_name.empty()) required_cols.insert(cond.rhs_col);
            }
            break;
        }
        case T_Projection:
            // A projection node itself doesn't require columns from its child for its *own* computation.
            // It defines the schema transformation. The columns its child needs to produce are determined by this projection's sel_cols.
            // This function is about what the *node operation itself* needs (e.g. filter conditions, join conditions).
            break; 
        case T_Aggregate: {
            auto p = std::static_pointer_cast<AggregatePlan>(plan);
            // 1. Columns in GROUP BY clause are required from the child.
            for (const auto& gb_col : p->group_bys_) {
                if (!gb_col.col_name.empty()) required_cols.insert(gb_col);
            }

            // 2. Columns that are arguments to aggregate functions.
            //    p->sel_cols_ are the output columns. p->agg_types_ tells us if it's an aggregate.
            //    If p->agg_types_[i] is an aggregate function, then p->sel_cols_[i] is assumed to be the input column.
            for (size_t i = 0; i < p->sel_cols_.size(); ++i) {
                if (i < p->agg_types_.size() && p->agg_types_[i] != AGG_COL) { 
                    // This sel_col is an input to an aggregate function (e.g. MAX(colA), SUM(colB))
                    // Skip COUNT(*) which might have empty col_name
                    if (!p->sel_cols_[i].col_name.empty()) {
                         required_cols.insert(p->sel_cols_[i]);
                    }
                }
                // If p->agg_types_[i] == AGG_COL, it's a group by key passed through.
                // These are already covered by adding all p->group_bys_. If not, add here.
                // However, typically, group_by columns that are also selected are part of p->group_bys_.
            }
            
            // 3. Columns in HAVING clause.
            for (const auto& hav_cond : p->havings_) {
                 // If LHS is a direct column (not an aggregate result, indicated by AGG_COL)
                 if (hav_cond.agg_type == AGG_COL && !hav_cond.lhs_col.col_name.empty()) {
                    required_cols.insert(hav_cond.lhs_col);
                 } 
                 // If LHS is an aggregate function (e.g. SUM(col) > 10), its input column is needed.
                 else if (hav_cond.agg_type != AGG_COL && !hav_cond.lhs_col.col_name.empty()) {
                    required_cols.insert(hav_cond.lhs_col); // lhs_col is the input to the agg func in having
                 }
                 // If RHS is a column
                 if (!hav_cond.is_rhs_val && !hav_cond.rhs_col.col_name.empty()) {
                    required_cols.insert(hav_cond.rhs_col);
                 }
            }
            break;
        }
        case T_Sort: {
            auto p = std::static_pointer_cast<SortPlan>(plan);
            if (!p->sel_col_.col_name.empty()) required_cols.insert(p->sel_col_); // Sort key
            break;
        }
        case T_SeqScan:
        case T_IndexScan:
            // Scan nodes are leaves in terms of requiring columns from children.
            // They provide columns but don't require any from further down.
            break; 
        default:
            // Other plan types (DDL, DML wrappers, OtherPlan) might not have columns 
            // required from children in the context of projection pushdown for select queries.
            break;
    }
    return required_cols;
}

// --- End of Helper methods for Projection Pushdown ---


std::shared_ptr<Plan> Planner::physical_optimization(std::shared_ptr<Query> query, Context *context) {
    std::shared_ptr<Plan> plan = make_one_rel(query);

    // Other physical optimization (like predicate pushdown, join algorithm selection)
    // Predicate pushdown is called in generate_select_plan after initial plan generation.
    
    // Join algorithm selection is partly in make_one_rel and could be further refined here.

    // Handle order by
    plan = generate_sort_plan(query, std::move(plan));

    return plan;
}

std::shared_ptr<Plan> Planner::make_one_rel(std::shared_ptr<Query> query) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    std::vector<std::string> tables = query->tables;
    // // Scan table , 生成表算子列表tab_nodes
    std::vector<std::shared_ptr<Plan> > table_scan_executors(tables.size());
    for (size_t i = 0; i < tables.size(); i++) {
        // 检查表上的谓词，连接谓词要后面查
        auto curr_conds = pop_conds(query->conds, tables[i]);
        // int index_no = get_indexNo(tables[i], curr_conds);
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(tables[i], curr_conds, index_col_names);
        
        std::shared_ptr<Plan> current_scan;
        if (index_exist) {
            current_scan = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, tables[i], index_col_names);
            // Check if sort can be avoided due to index order
            if (x->has_sort) { // x is ast::SelectStmt
                // This logic might need refinement: checking if index covers sort order.
                // For now, if any condition used for index matches sort_by, assume sort might be avoidable.
                // A more robust check would compare index_col_names with query->sort_bys.
                for (auto const& cond_col_name : index_col_names) {
                     if (query->sort_bys.col_name == cond_col_name && query->sort_bys.tab_name == tables[i]) {
                        // This is a simplification. True sort avoidance depends on whether the index order matches the required sort order.
                        // And if all preceding index columns are equality conditions.
                        // x->has_sort = false; // Potentially avoid sort.
                        break;
                    }
                }
            }
        } else {
            index_col_names.clear(); // Ensure it's clear if no index
            current_scan = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, tables[i]);
        }

        if (!curr_conds.empty()) {
            table_scan_executors[i] = std::make_shared<FilterPlan>(current_scan, curr_conds);
        } else {
            table_scan_executors[i] = current_scan;
        }
    }
    // 只有一个表，不需要join。
    if (tables.size() == 1) {
        // The single table plan might be a ScanPlan or FilterPlan(ScanPlan)
        return table_scan_executors[0]; 
    }
    // 获取where条件
    auto conds = std::move(query->conds);
    std::shared_ptr<Plan> table_join_executors;

    int scantbl[tables.size()];
    for (size_t i = 0; i < tables.size(); i++) {
        scantbl[i] = -1;
    }
    // 假设在ast中已经添加了jointree，这里需要修改的逻辑是，先处理jointree，然后再考虑剩下的部分
    if (conds.size() >= 1) {
        // 有连接条件

        // 根据连接条件，生成第一层join
        std::vector<std::string> joined_tables(tables.size());
        auto it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left, right;
            std::vector<Condition> join_conds{*it};
            left = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables, table_scan_executors);
            right = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables, table_scan_executors);

            // The following section for upgrading ScanPlans to IndexScans based on join conditions
            // needs careful review. A ScanPlan is already either Seq or Index based on its own filters.
            // Forcing it to IndexScan here based on join conditions might be incorrect if it was a FilterPlan(SeqScan).
            // This suggests that Index Nested Loop Join logic should be more explicit.
            // For now, we assume `left` and `right` are the base scan nodes (or Filter(Scan)).
            // The `get_index_cols` calls below are intended to see if these scans can *use* an index
            // for the join operation itself (e.g. inner side of an INLJ).
            // This part of the code is complex and might need a dedicated refactor for INLJ.
            // The current changes focus on ensuring ScanPlans don't filter and FilterPlans do.

            // Attempt to use index for left side of join if applicable (less common for outer loop)
            // auto left_scan_node = (left->tag == T_Filter) ? std::dynamic_pointer_cast<FilterPlan>(left)->child_ : left;
            // if (auto left_scan = std::dynamic_pointer_cast<ScanPlan>(left_scan_node)) {
            //     if (left_scan->tag == T_SeqScan) { // Only try to upgrade if it's currently a SeqScan
            //         std::vector<std::string> left_index_cols;
            //         // Note: join_conds here are specific to this join.
            //         // get_index_cols might modify join_conds, which could be problematic.
            //         // A copy might be safer: auto temp_join_conds_for_left_idx = join_conds;
            //         if (get_index_cols(it->lhs_col.tab_name, join_conds, left_index_cols)) {
            //             // This implies we are trying to make the left an IndexScan for the join.
            //             // This is unusual for the outer table of a join. Potentially remove or rethink.
            //         }
            //     }
            // }

            // Attempt to use index for right side of join (common for inner loop of INLJ)
            // auto right_scan_node = (right->tag == T_Filter) ? std::dynamic_pointer_cast<FilterPlan>(right)->child_ : right;
            // if (auto right_scan = std::dynamic_pointer_cast<ScanPlan>(right_scan_node)) {
            //      if (right_scan->tag == T_SeqScan) { // Only try to upgrade if it's currently a SeqScan
            //         std::vector<std::string> right_index_cols;
            //         auto temp_join_conds_for_right_idx = join_conds; // Use a copy
            //         // Need to ensure conditions are appropriate for the right table (e.g. swap lhs/rhs if needed)
            //         // This is highly complex; for now, we assume get_index_cols handles it or it's simplified.
            //         if (get_index_cols(it->rhs_col.tab_name, temp_join_conds_for_right_idx, right_index_cols)) {
            //             // This would mean the right scan becomes an IndexScan using these join conditions.
            //             // The original `right` plan might need to be replaced.
            //             // e.g., right = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, it->rhs_col.tab_name, right_index_cols);
            //             // And if `right` was part of a FilterPlan, that FilterPlan would now wrap this new IndexScan.
            //             // This is a placeholder for where more sophisticated INLJ logic would go.
            //         }
            //      }
            // }
            
            // Sort optimization logic (original logic is kept, but interacts with new plan structure)
            // This also needs care: left/right could be FilterPlans. Tag check should be on the underlying ScanPlan.
            PlanTag left_tag_for_sort = left->tag;
            if(left_tag_for_sort == T_Filter) left_tag_for_sort = std::dynamic_pointer_cast<FilterPlan>(left)->child_->tag;
            PlanTag right_tag_for_sort = right->tag;
            if(right_tag_for_sort == T_Filter) right_tag_for_sort = std::dynamic_pointer_cast<FilterPlan>(right)->child_->tag;

            if (x->has_sort) { // x is ast::SelectStmt
                bool sort_on_left = (join_conds[0].lhs_col == query->sort_bys);
                bool sort_on_right = (join_conds[0].rhs_col == query->sort_bys);

                if ( (left_tag_for_sort != T_IndexScan && right_tag_for_sort == T_IndexScan && sort_on_left) ||
                     (left_tag_for_sort == T_IndexScan && right_tag_for_sort != T_IndexScan && sort_on_right) ) {
                    // One side is indexed (for join or otherwise), other is not and needs sort for merge join or ordered output
                    if (sort_on_left && left_tag_for_sort != T_IndexScan) {
                        left = std::make_shared<SortPlan>(T_Sort, std::move(left), it->lhs_col, x->order->orderby_dir == ast::OrderBy_DESC);
                    } else if (sort_on_right && right_tag_for_sort != T_IndexScan) {
                        right = std::make_shared<SortPlan>(T_Sort, std::move(right), it->rhs_col, x->order->orderby_dir == ast::OrderBy_DESC);
                    }
                    if(sort_on_left || sort_on_right) x->has_sort = false;
                } else if (left_tag_for_sort != T_IndexScan && right_tag_for_sort != T_IndexScan && (sort_on_left || sort_on_right)) {
                     // Both sides need sort
                    left = std::make_shared<SortPlan>(T_Sort, std::move(left), it->lhs_col, x->order->orderby_dir == ast::OrderBy_DESC);
                    right = std::make_shared<SortPlan>(T_Sort, std::move(right), it->rhs_col, x->order->orderby_dir == ast::OrderBy_DESC);
                    if(sort_on_left || sort_on_right) x->has_sort = false;
                } else if (left_tag_for_sort == T_IndexScan && right_tag_for_sort == T_IndexScan && (sort_on_left || sort_on_right)) {
                    // Both sides are indexed, sort potentially avoided if index order matches
                     if(sort_on_left || sort_on_right) x->has_sort = false;
                }
            }
            
            // Determine Join Type (NestedLoop vs SortMerge)
            // This logic also needs to check underlying scan type if plans are FilterPlans
            PlanTag final_left_tag = left->tag;
            if(final_left_tag == T_Filter) final_left_tag = std::dynamic_pointer_cast<FilterPlan>(left)->child_->tag;
            PlanTag final_right_tag = right->tag;
            if(final_right_tag == T_Filter) final_right_tag = std::dynamic_pointer_cast<FilterPlan>(right)->child_->tag;

            // Build JoinPlan
            if (enable_sortmerge_join && (final_left_tag == T_Sort || final_left_tag == T_IndexScan) &&
                                          (final_right_tag == T_Sort || final_right_tag == T_IndexScan)) {
                table_join_executors = std::make_shared<JoinPlan>(T_SortMerge, std::move(left), std::move(right), std::move(join_conds));
            } else if (enable_nestedloop_join) {
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), std::move(join_conds));
            } else if (enable_sortmerge_join) { // Fallback to NLJ if SortMerge requirements not met but was preferred
                 table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left), std::move(right), std::move(join_conds));
            }
            else {
                throw RMDBError("No join executor selected or enabled!");
            }
            it = conds.erase(it); // Remove the processed join condition
            break; // Process one join condition to form the initial join pair
        }
        // 根据连接条件，生成第2-n层join
        it = conds.begin();
        while (it != conds.end()) {
            std::shared_ptr<Plan> left_need_to_join_executors = nullptr;
            std::shared_ptr<Plan> right_need_to_join_executors = nullptr;
            bool isneedreverse = false;
            if (std::find(joined_tables.begin(), joined_tables.end(), it->lhs_col.tab_name) == joined_tables.end()) {
                left_need_to_join_executors = pop_scan(scantbl, it->lhs_col.tab_name, joined_tables,
                                                       table_scan_executors);
            }
            if (std::find(joined_tables.begin(), joined_tables.end(), it->rhs_col.tab_name) == joined_tables.end()) {
                right_need_to_join_executors = pop_scan(scantbl, it->rhs_col.tab_name, joined_tables,
                                                        table_scan_executors);
                isneedreverse = true;
            }

            if (left_need_to_join_executors != nullptr && right_need_to_join_executors != nullptr) {
                std::vector<Condition> join_conds{*it};
                std::shared_ptr<Plan> temp_join_executors = std::make_shared<JoinPlan>(T_NestLoop,
                    std::move(left_need_to_join_executors),
                    std::move(right_need_to_join_executors),
                    join_conds);
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(temp_join_executors),
                                                                  std::move(table_join_executors),
                                                                  std::vector<Condition>());
            } else if (left_need_to_join_executors != nullptr || right_need_to_join_executors != nullptr) {
                if (isneedreverse) {
                    std::map<CompOp, CompOp> swap_op = {
                        {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
                    };
                    std::swap(it->lhs_col, it->rhs_col);
                    it->op = swap_op.at(it->op);
                    left_need_to_join_executors = std::move(right_need_to_join_executors);
                }
                std::vector<Condition> join_conds{*it};
                table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(left_need_to_join_executors),
                                                                  std::move(table_join_executors), join_conds);
            } else {
                push_conds(std::move(&(*it)), table_join_executors);
            }
            it = conds.erase(it);
        }
    } else {
        table_join_executors = table_scan_executors[0];
        scantbl[0] = 1;
    }

    // 连接剩余表
    for (size_t i = 0; i < tables.size(); i++) {
        if (scantbl[i] == -1) {
            table_join_executors = std::make_shared<JoinPlan>(T_NestLoop, std::move(table_scan_executors[i]),
                                                              std::move(table_join_executors),
                                                              std::vector<Condition>());
        }
    }

    return table_join_executors;
}

std::shared_ptr<Plan> Planner::generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan) {
    auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse);
    if (!x->has_sort) {
        return plan;
    }
    // std::vector<ColMeta> all_cols;
    // for (auto &sel_tab_name: query->tables) {
    //     // 这里db_不能写成get_db(), 注意要传指针
    //     const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
    //     all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    // }
    // TabCol sel_col;
    // // TODO 支持多列排序
    // for (auto &col: all_cols) {
    //     if (col.name == x->order->cols->col_name) {
    //         sel_col = {.tab_name = col.tab_name, .col_name = col.name};
    //     }
    // }
    return std::make_shared<SortPlan>(T_Sort, std::move(plan), std::move(query->sort_bys),
                                      x->order->orderby_dir == ast::OrderBy_DESC);
}

/**
 * @brief select plan 生成
 *
 * @param sel_cols select plan 选取的列
 * @param tab_names select plan 目标的表
 * @param conds select plan 选取条件
 */
std::shared_ptr<Plan> Planner::generate_select_plan(std::shared_ptr<Query> query, Context *context) {
    // 逻辑优化 (currently a placeholder)
    query = logical_optimization(std::move(query), context);

    // Initial physical plan generation
    auto sel_cols = query->cols; // Keep selected columns info
    std::shared_ptr<Plan> plannerRoot = physical_optimization(query, context); // Builds basic scans, joins

    // Apply predicate pushdown
    plannerRoot = push_down_filters(plannerRoot);

    // Check if aggregation is needed
    bool is_agg = !query->group_bys.empty();
    if (!is_agg) {
        for (auto &agg_type: query->agg_types) {
            if (agg_type != AGG_COL) {
                is_agg = true;
                break;
            }
        }
    }

    // 生成聚合计划
    if (is_agg) {
        plannerRoot = std::make_shared<AggregatePlan>(T_Aggregate, std::move(plannerRoot), query->cols,
                                                      query->agg_types, query->group_bys, query->havings);
    }

    // TODO 待会处理别名
    plannerRoot = std::make_shared<ProjectionPlan>(T_Projection, std::move(plannerRoot),
                                                   std::move(sel_cols), std::move(query->alias));

    return plannerRoot;
}

// 生成DDL语句和DML语句的查询执行计划
std::shared_ptr<Plan> Planner::do_planner(std::shared_ptr<Query> query, Context *context) {
    std::shared_ptr<Plan> plannerRoot;
    if (auto x = std::dynamic_pointer_cast<ast::CreateTable>(query->parse)) {
        // create table;
        std::vector<ColDef> col_defs;
        for (auto &field: x->fields) {
            if (auto sv_col_def = std::dynamic_pointer_cast<ast::ColDef>(field)) {
                ColDef col_def = {
                    .name = sv_col_def->col_name,
                    .type = interp_sv_type(sv_col_def->type_len->type),
                    .len = sv_col_def->type_len->len
                };
                col_defs.push_back(col_def);
            } else {
                throw InternalError("Unexpected field type");
            }
        }
        plannerRoot = std::make_shared<DDLPlan>(T_CreateTable, x->tab_name, std::vector<std::string>(), col_defs);
    } else if (auto x = std::dynamic_pointer_cast<ast::DropTable>(query->parse)) {
        // drop table;
        plannerRoot = std::make_shared<DDLPlan>(T_DropTable, x->tab_name, std::vector<std::string>(),
                                                std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::CreateIndex>(query->parse)) {
        // create index;
        plannerRoot = std::make_shared<DDLPlan>(T_CreateIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DropIndex>(query->parse)) {
        // drop index
        plannerRoot = std::make_shared<DDLPlan>(T_DropIndex, x->tab_name, x->col_names, std::vector<ColDef>());
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(query->parse)) {
        // insert;
        plannerRoot = std::make_shared<DMLPlan>(T_Insert, std::shared_ptr<Plan>(), x->tab_name,
                                                query->values, std::vector<Condition>(), std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(query->parse)) {
        // delete;
        // 生成表扫描方式
        std::shared_ptr<Plan> table_scan_executors;
        // 只有一张表，不需要进行物理优化了
        // int index_no = get_indexNo(x->tab_name, query->conds);
        std::vector<Condition> dml_conditions = query->conds; // Conditions specific to this DML
        std::vector<std::string> index_col_names;
        // get_index_cols might modify dml_conditions, separating index-usable parts from filter parts.
        bool index_exist = get_index_cols(x->tab_name, dml_conditions, index_col_names);

        std::shared_ptr<Plan> scan_node;
        if (index_exist) {
            scan_node = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, index_col_names);
        } else {
            scan_node = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name);
        }
        // Apply remaining conditions (if any) as a filter on top of the scan
        table_scan_executors = add_filter_if_needed(scan_node, dml_conditions);
        
        // The DMLPlan itself. Conditions for DML are often handled by its subplan (table_scan_executors here).
        // The query->conds passed to DMLPlan constructor might be redundant if already handled by FilterPlan.
        // For now, keeping query->conds as per original structure, but it's worth noting.
        plannerRoot = std::make_shared<DMLPlan>(T_Delete, table_scan_executors, x->tab_name,
                                                std::vector<Value>(), query->conds, std::vector<SetClause>());
    } else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(query->parse)) {
        // update;
        std::vector<Condition> dml_conditions = query->conds;
        std::vector<std::string> index_col_names;
        bool index_exist = get_index_cols(x->tab_name, dml_conditions, index_col_names);
        
        std::shared_ptr<Plan> scan_node;
        if (index_exist) {
            scan_node = std::make_shared<ScanPlan>(T_IndexScan, sm_manager_, x->tab_name, index_col_names);
        } else {
            scan_node = std::make_shared<ScanPlan>(T_SeqScan, sm_manager_, x->tab_name);
        }
        
        std::shared_ptr<Plan> table_scan_executors_for_update = add_filter_if_needed(scan_node, dml_conditions);
        
        plannerRoot = std::make_shared<DMLPlan>(T_Update, table_scan_executors_for_update, x->tab_name,
                                                std::vector<Value>(), query->conds,
                                                query->set_clauses);
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(query->parse)) {
        // std::shared_ptr<plannerInfo> root = std::make_shared<plannerInfo>(x); // Not used elsewhere, can be removed if not needed
        // generate_select_plan handles the main logic for SELECT queries, including predicate pushdown
        std::shared_ptr<Plan> projection = generate_select_plan(std::move(query), context);
        plannerRoot = std::make_shared<DMLPlan>(T_select, projection, std::string(), std::vector<Value>(),
                                                std::vector<Condition>(), std::vector<SetClause>());
    } else {
        throw InternalError("Unexpected AST root");
    }
    return plannerRoot;
}
