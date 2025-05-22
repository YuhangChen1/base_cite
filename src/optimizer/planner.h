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
#include <vector>

#include "execution/execution_defs.h"
#include "execution/execution_manager.h"
#include "record/rm.h"
#include "system/sm.h"
#include "common/context.h"
#include "plan.h"
#include "parser/parser.h"
#include "common/common.h" // Ensure TabCol is defined here
#include "analyze/analyze.h"
#include <unordered_set> // For std::unordered_set

// Hash and Equality for TabCol to be used in std::unordered_set
struct TabColHash {
    std::size_t operator()(const TabCol& tc) const {
        auto hash1 = std::hash<std::string>()(tc.tab_name);
        auto hash2 = std::hash<std::string>()(tc.col_name);
        return hash1 ^ (hash2 << 1); 
    }
};

struct TabColEqual {
    bool operator()(const TabCol& a, const TabCol& b) const {
        return a.tab_name == b.tab_name && a.col_name == b.col_name;
    }
};

class Planner {
private:
    SmManager *sm_manager_;

    bool enable_nestedloop_join = false;
    bool enable_sortmerge_join = true;

public:
    Planner(SmManager *sm_manager) : sm_manager_(sm_manager) {
    }


    std::shared_ptr<Plan> do_planner(std::shared_ptr<Query> query, Context *context);

    void set_enable_nestedloop_join(bool set_val) { enable_nestedloop_join = set_val; }

    void set_enable_sortmerge_join(bool set_val) { enable_sortmerge_join = set_val; }

private:
    std::shared_ptr<Query> logical_optimization(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> physical_optimization(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> make_one_rel(std::shared_ptr<Query> query);

    std::shared_ptr<Plan> generate_sort_plan(std::shared_ptr<Query> query, std::shared_ptr<Plan> plan);

    std::shared_ptr<Plan> generate_select_plan(std::shared_ptr<Query> query, Context *context);

    std::shared_ptr<Plan> pop_scan(int *scantbl, const std::string &table, std::vector<std::string> &joined_tables,
                                   std::vector<std::shared_ptr<Plan> > plans);

    // int get_indexNo(std::string tab_name, std::vector<Condition> curr_conds);
    bool get_index_cols(std::string &tab_name, std::vector<Condition> &curr_conds,
                        std::vector<std::string> &index_col_names);

    // Helper methods for predicate pushdown
    std::set<std::string> get_plan_tables(const std::shared_ptr<Plan>& plan);
    bool condition_uses_only_tables(const Condition& cond, const std::set<std::string>& tables);
    std::shared_ptr<Plan> add_filter_if_needed(std::shared_ptr<Plan> plan, const std::vector<Condition>& conditions);
    std::shared_ptr<Plan> push_down_filters(std::shared_ptr<Plan> plan);

    // Helper methods for projection pushdown
    std::shared_ptr<Plan> push_down_projections(std::shared_ptr<Plan> plan, const std::unordered_set<TabCol, TabColHash, TabColEqual>& required_cols_by_parent);
    std::unordered_set<TabCol, TabColHash, TabColEqual> get_cols_required_by_node_itself(const std::shared_ptr<Plan>& plan); // Renamed for clarity
    std::shared_ptr<Plan> add_projection_if_needed(std::shared_ptr<Plan> current_plan_node, const std::unordered_set<TabCol, TabColHash, TabColEqual>& required_cols_for_this_projection);
    std::vector<TabCol> convert_unordered_set_to_vector(const std::unordered_set<TabCol, TabColHash, TabColEqual>& cols_set);
    bool column_is_from_tables(const TabCol& tc, const std::set<std::string>& tables); 


    ColType interp_sv_type(ast::SvType sv_type) {
        std::map<ast::SvType, ColType> m = {
            {ast::SV_TYPE_INT, TYPE_INT}, {ast::SV_TYPE_FLOAT, TYPE_FLOAT}, {ast::SV_TYPE_STRING, TYPE_STRING}
        };
        return m.at(sv_type);
    }
};
