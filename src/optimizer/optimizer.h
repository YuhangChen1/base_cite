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

#include <map>

#include "errors.h"
#include "execution/execution.h"
#include "parser/parser.h"
#include "system/sm.h"
#include "common/context.h"
#include "transaction/transaction_manager.h"
#include "planner.h"
#include "plan.h"

class Optimizer {
private:
    SmManager *sm_manager_;
    Planner *planner_;

public:
    Optimizer(SmManager *sm_manager, Planner *planner)
        : sm_manager_(sm_manager), planner_(planner) {
    }

    std::shared_ptr<Plan> plan_query(std::shared_ptr<Query> query, Context *context) {
        if (auto explain_stmt = std::dynamic_pointer_cast<ast::ExplainStmt>(query->parse)) {
            // Handle EXPLAIN statement
            auto inner_query_ast = explain_stmt->query_stmt;

            // Create a Query object for the statement to be explained
            // The Query object typically holds the AST and other derived information.
            // For the inner query of EXPLAIN, we primarily need to pass its AST.
            // The Planner's do_planner method is expected to perform any necessary
            // analysis on this AST (e.g., populating table names, conditions)
            // if it hasn't been done already for the inner_query_ast.
            std::shared_ptr<Query> inner_query_obj = std::make_shared<Query>();
            inner_query_obj->parse = inner_query_ast;
            // If the original query object (wrapper for EXPLAIN) had other relevant info
            // that should apply to the inner query (like transaction context),
            // it might need to be copied or passed along. For now, a minimal Query obj.

            // Get the optimized plan for the inner query
            std::shared_ptr<Plan> optimized_inner_plan = planner_->do_planner(inner_query_obj, context);

            // Wrap the inner plan in an ExplainPlan
            return std::make_shared<ExplainPlan>(optimized_inner_plan);
        }
        if (auto x = std::dynamic_pointer_cast<ast::Help>(query->parse)) {
            // help;
            return std::make_shared<OtherPlan>(T_Help, std::string());
        }
        if (auto x = std::dynamic_pointer_cast<ast::ShowTables>(query->parse)) {
            // show tables;
            return std::make_shared<OtherPlan>(T_ShowTable, "");
        }
        if (auto x = std::dynamic_pointer_cast<ast::ShowIndexs>(query->parse)) {
            // show indexs;
            return std::make_shared<OtherPlan>(T_ShowIndex, x->tab_name);
        }
        if (auto x = std::dynamic_pointer_cast<ast::DescTable>(query->parse)) {
            // desc table;
            return std::make_shared<OtherPlan>(T_DescTable, x->tab_name);
        }
        if (auto x = std::dynamic_pointer_cast<ast::TxnBegin>(query->parse)) {
            // begin;
            return std::make_shared<OtherPlan>(T_Transaction_begin, std::string());
        }
        if (auto x = std::dynamic_pointer_cast<ast::TxnAbort>(query->parse)) {
            // abort;
            return std::make_shared<OtherPlan>(T_Transaction_abort, std::string());
        }
        if (auto x = std::dynamic_pointer_cast<ast::TxnCommit>(query->parse)) {
            // commit;
            return std::make_shared<OtherPlan>(T_Transaction_commit, std::string());
        }
        if (auto x = std::dynamic_pointer_cast<ast::TxnRollback>(query->parse)) {
            // rollback;
            return std::make_shared<OtherPlan>(T_Transaction_rollback, std::string());
        }
        if (auto x = std::dynamic_pointer_cast<ast::SetStmt>(query->parse)) {
            // Set Knob Plan
            return std::make_shared<SetKnobPlan>(x->set_knob_type_, x->bool_val_);
        }
        return planner_->do_planner(query, context);
    }
};
