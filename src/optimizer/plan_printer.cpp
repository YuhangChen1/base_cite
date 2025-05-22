#include "optimizer/plan_printer.h"
#include "optimizer/plan.h"
#include "common/common.h" // For Condition, Value, CompOp, TabCol etc.

#include <algorithm> // For std::sort
#include <vector>    // For std::vector
#include <functional> // For std::function (used in collect_join_tables)
#include <set>       // For std::set (used in collect_join_tables)
#include <string>    // For std::to_string, std::string manipulation

// Helper function to generate indentation string (already present from previous version)
static std::string indent_str(int level) {
    std::string s;
    for (int i = 0; i < level; ++i) {
        s += "\t"; // Use literal tab for output as per requirement
    }
    return s;
}

std::string PlanPrinter::format_op(CompOp op) {
    switch (op) {
        case OP_EQ: return "=";
        case OP_NE: return "!=";
        case OP_LT: return "<";
        case OP_GT: return ">";
        case OP_LE: return "<=";
        case OP_GE: return ">=";
        default: return "?OP?";
    }
}

std::string PlanPrinter::value_to_string(const Value& val) {
    if (val.type == TYPE_INT) return std::to_string(val.int_val);
    if (val.type == TYPE_FLOAT) return std::to_string(val.float_val); // Consider std::fixed, std::setprecision for floats if needed
    if (val.type == TYPE_STRING) return "'" + val.str_val + "'";
    if (val.type == TYPE_BOOL) return val.bool_val ? "true" : "false";
    return "UNKN_VAL(" + std::to_string(val.type) + ")";
}

std::string PlanPrinter::format_condition_to_string(const Condition& cond) {
    std::string lhs_str;
    // COUNT(*) case for having clause
    if (cond.agg_type == AGG_COUNT && cond.lhs_col.tab_name.empty() && cond.lhs_col.col_name.empty()) {
        lhs_str = "COUNT(*)";
    } else {
        // Standard column
        lhs_str = cond.lhs_col.tab_name + "." + cond.lhs_col.col_name;
        // Handle other aggregate types if necessary, e.g., SUM(t.col), MAX(t.col)
        // For now, this assumes lhs_col is directly usable or it's COUNT(*)
    }

    std::string op_str = format_op(cond.op);
    std::string rhs_str;
    if (cond.is_rhs_val) {
        rhs_str = value_to_string(cond.rhs_val);
    } else {
        rhs_str = cond.rhs_col.tab_name + "." + cond.rhs_col.col_name;
    }
    return lhs_str + op_str + rhs_str;
}

void PlanPrinter::collect_join_tables(const std::shared_ptr<Plan>& plan, std::set<std::string>& table_names_set) {
    if (!plan) return;

    switch (plan->tag) {
        case T_SeqScan:
        case T_IndexScan: {
            auto p = std::dynamic_pointer_cast<ScanPlan>(plan);
            table_names_set.insert(p->tab_name_);
            break;
        }
        case T_Filter: {
            auto p = std::dynamic_pointer_cast<FilterPlan>(plan);
            collect_join_tables(p->child_, table_names_set);
            break;
        }
        case T_Projection: {
            auto p = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            collect_join_tables(p->subplan_, table_names_set);
            break;
        }
        case T_Sort: {
             auto p = std::dynamic_pointer_cast<SortPlan>(plan);
             collect_join_tables(p->subplan_, table_names_set);
             break;
        }
        case T_Aggregate: {
            auto p = std::dynamic_pointer_cast<AggregatePlan>(plan);
            collect_join_tables(p->subplan_, table_names_set);
            break;
        }
        case T_NestLoop:
        case T_SortMerge: { // Assuming SortMergeJoin uses JoinPlan
            auto p = std::dynamic_pointer_cast<JoinPlan>(plan);
            collect_join_tables(p->left_, table_names_set);
            collect_join_tables(p->right_, table_names_set);
            break;
        }
        // For DML plans, they might wrap a Select-like structure or act on a single table.
        // If they wrap a subplan that could be part of a join, recurse.
        // If they act on a table directly (e.g. Insert into specific table), that table isn't part of a "join" table set.
        // This function is for "Join(tables=...)" so DML tables are not collected here.
        default:
            // Other plan types (like DDL, DML wrappers, OtherPlan) are not expected to contribute table names to a Join's table list.
            break;
    }
}


std::string PlanPrinter::print(const std::shared_ptr<Plan>& plan) {
    std::string output = "";
    if (!plan) {
        return "Empty plan\n";
    }
    print_node(plan, output, 0);
    return output;
}

void PlanPrinter::print_node(const std::shared_ptr<Plan>& plan, std::string& output, int indent_level) {
    if (!plan) return;

    output += indent_str(indent_level);

    switch (plan->tag) {
        case T_SeqScan:
        case T_IndexScan: {
            auto p = std::dynamic_pointer_cast<ScanPlan>(plan);
            output += "Scan(table=" + p->tab_name_ + ")";
            // Index name for T_IndexScan is not part of the spec for "Scan" node.
            // if (plan->tag == T_IndexScan && !p->index_col_names_.empty()) {
            //     output += " using index " + p->index_col_names_[0] + " (example)"; // Not in spec
            // }
            output += "\n";
            break;
        }
        case T_Filter: {
            auto p = std::dynamic_pointer_cast<FilterPlan>(plan);
            output += "Filter(condition=[";
            std::vector<std::string> cond_strs;
            for (const auto& cond : p->conditions_) {
                cond_strs.push_back(format_condition_to_string(cond));
            }
            std::sort(cond_strs.begin(), cond_strs.end());
            for (size_t i = 0; i < cond_strs.size(); ++i) {
                output += cond_strs[i] + (i == cond_strs.size() - 1 ? "" : ",");
            }
            output += "])\n";
            print_node(p->child_, output, indent_level + 1);
            break;
        }
        case T_Projection: {
            auto p = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            output += "Project(columns=[";
            // Check for SELECT * representation (e.g., single TabCol with col_name == "*")
            if (p->sel_cols_.size() == 1 && p->sel_cols_[0].col_name == "*" && p->sel_cols_[0].tab_name.empty()) {
                output += "*";
            } else {
                std::vector<std::string> col_strs;
                for (const auto& tc : p->sel_cols_) {
                    // Assuming tc.tab_name is populated. If not, logic to fetch it or omit prefix might be needed.
                    col_strs.push_back(tc.tab_name + "." + tc.col_name);
                }
                std::sort(col_strs.begin(), col_strs.end());
                for (size_t i = 0; i < col_strs.size(); ++i) {
                    output += col_strs[i] + (i == col_strs.size() - 1 ? "" : ",");
                }
            }
            output += "])\n";
            print_node(p->subplan_, output, indent_level + 1);
            break;
        }
        case T_NestLoop:
        case T_SortMerge: {
            auto p = std::dynamic_pointer_cast<JoinPlan>(plan);
            output += "Join(tables=[";
            
            std::set<std::string> table_names_set;
            collect_join_tables(p->left_, table_names_set);
            collect_join_tables(p->right_, table_names_set);
            
            std::vector<std::string> table_names(table_names_set.begin(), table_names_set.end());
            // std::sort is not needed as std::set iterates in sorted order.
            // std::sort(table_names.begin(), table_names.end()); // Already sorted by std::set
            
            for (size_t i = 0; i < table_names.size(); ++i) {
                output += table_names[i] + (i == table_names.size() - 1 ? "" : ",");
            }
            output += "], condition=[";
            
            std::vector<std::string> cond_strs;
            for (const auto& cond : p->conds_) {
                cond_strs.push_back(format_condition_to_string(cond));
            }
            std::sort(cond_strs.begin(), cond_strs.end());
            for (size_t i = 0; i < cond_strs.size(); ++i) {
                output += cond_strs[i] + (i == cond_strs.size() - 1 ? "" : ",");
            }
            output += "])\n";
            print_node(p->left_, output, indent_level + 1);
            print_node(p->right_, output, indent_level + 1);
            break;
        }
        case T_Sort: {
            auto p = std::dynamic_pointer_cast<SortPlan>(plan);
            output += "Sort(key=" + p->sel_col_.tab_name + "." + p->sel_col_.col_name + (p->is_desc_ ? " DESC" : " ASC") + ")\n";
            print_node(p->subplan_, output, indent_level + 1);
            break;
        }
        case T_Aggregate: {
            // Basic output, spec for AggregateNode format is not given in this task
            auto p = std::dynamic_pointer_cast<AggregatePlan>(plan);
            output += "Aggregate(group_by=[";
            std::vector<std::string> group_by_strs;
            for(const auto& gb_col : p->group_bys_) {
                group_by_strs.push_back(gb_col.tab_name + "." + gb_col.col_name);
            }
            std::sort(group_by_strs.begin(), group_by_strs.end());
            for (size_t i = 0; i < group_by_strs.size(); ++i) {
                output += group_by_strs[i] + (i == group_by_strs.size() - 1 ? "" : ",");
            }
            output += "], aggregates=[";
            // aggregates (sel_cols_ with agg_types_)
            std::vector<std::string> agg_strs;
            for(size_t i = 0; i < p->sel_cols_.size(); ++i) {
                std::string agg_str;
                // This needs to map AggType to string e.g. COUNT, SUM etc.
                // And handle COUNT(*) case where col_name might be empty.
                // For now, a placeholder:
                if(p->agg_types_[i] == AGG_COUNT && p->sel_cols_[i].col_name.empty()) {
                     agg_str = "COUNT(*)";
                } else if (p->agg_types_[i] != AGG_COL) {
                    agg_str = AggTypeStr(p->agg_types_[i]) + "(" + p->sel_cols_[i].tab_name + "." + p->sel_cols_[i].col_name + ")";
                } else {
                    agg_str = p->sel_cols_[i].tab_name + "." + p->sel_cols_[i].col_name; // Non-aggregated column in group by
                }
                agg_strs.push_back(agg_str);
            }
            std::sort(agg_strs.begin(), agg_strs.end());
             for (size_t i = 0; i < agg_strs.size(); ++i) {
                output += agg_strs[i] + (i == agg_strs.size() - 1 ? "" : ",");
            }
            output += "]";

            if (!p->havings_.empty()){
                 output += ", having_condition=[";
                 std::vector<std::string> having_cond_strs;
                 for(const auto& hcond : p->havings_){
                     having_cond_strs.push_back(format_condition_to_string(hcond));
                 }
                 std::sort(having_cond_strs.begin(), having_cond_strs.end());
                 for (size_t i = 0; i < having_cond_strs.size(); ++i) {
                    output += having_cond_strs[i] + (i == having_cond_strs.size() - 1 ? "" : ",");
                }
                 output += "]";
            }
            output += ")\n";
            print_node(p->subplan_, output, indent_level + 1);
            break;
        }
        // DDL and Other plans (maintain simple output as spec not given for these)
        case T_Help: output += "HelpPlan\n"; break;
        case T_ShowTable: output += "ShowTablePlan\n"; break;
        case T_ShowIndex: {
            auto p = std::dynamic_pointer_cast<OtherPlan>(plan); // Assuming OtherPlan is used
            output += "ShowIndexPlan(table=" + p->tab_name_ + ")\n";
            break;
        }
        case T_DescTable: {
            auto p = std::dynamic_pointer_cast<OtherPlan>(plan); // Assuming OtherPlan is used
            output += "DescTablePlan(table=" + p->tab_name_ + ")\n";
            break;
        }
        case T_CreateTable: { // DDLPlan
            auto p = std::dynamic_pointer_cast<DDLPlan>(plan);
            output += "CreateTablePlan(table=" + p->tab_name_ + ")\n";
            break;
        }
        case T_DropTable: { // DDLPlan
            auto p = std::dynamic_pointer_cast<DDLPlan>(plan);
            output += "DropTablePlan(table=" + p->tab_name_ + ")\n";
            break;
        }
        case T_CreateIndex: { // DDLPlan
            auto p = std::dynamic_pointer_cast<DDLPlan>(plan);
            output += "CreateIndexPlan(table=" + p->tab_name_ + ", columns=[";
            for(size_t i=0; i<p->tab_col_names_.size(); ++i){
                output += p->tab_col_names_[i] + (i == p->tab_col_names_.size() - 1 ? "" : ",");
            }
            output += "])\n";
            break;
        }
        case T_DropIndex: { // DDLPlan
             auto p = std::dynamic_pointer_cast<DDLPlan>(plan);
            output += "DropIndexPlan(table=" + p->tab_name_ + ", columns=[";
             for(size_t i=0; i<p->tab_col_names_.size(); ++i){
                output += p->tab_col_names_[i] + (i == p->tab_col_names_.size() - 1 ? "" : ",");
            }
            output += "])\n";
            break;
        }
        case T_SetKnob: output += "SetKnobPlan\n"; break; 
        case T_Insert: { // DMLPlan
            auto p = std::dynamic_pointer_cast<DMLPlan>(plan);
            output += "InsertPlan(table=" + p->tab_name_ + ")\n";
            // Values are not typically shown in EXPLAIN for Insert
            break;
        }
        case T_Update: { // DMLPlan
            auto p = std::dynamic_pointer_cast<DMLPlan>(plan);
            output += "UpdatePlan(table=" + p->tab_name_ + ")\n";
            // Set clauses and conditions are part of the subplan (Filter/Scan)
            print_node(p->subplan_, output, indent_level + 1);
            break;
        }
        case T_Delete: { // DMLPlan
            auto p = std::dynamic_pointer_cast<DMLPlan>(plan);
            output += "DeletePlan(table=" + p->tab_name_ + ")\n";
            // Conditions are part of the subplan (Filter/Scan)
            print_node(p->subplan_, output, indent_level + 1);
            break;
        }
        case T_select: { // DMLPlan that wraps a select query execution
            auto p = std::dynamic_pointer_cast<DMLPlan>(plan);
            // This node itself is conceptual for select execution.
            // The actual query structure (Project, Join, etc.) is in its subplan.
            // So, we just recurse. If this T_select node should have a specific output
            // like "SelectQuery", it can be added. For now, assume it's a wrapper.
            print_node(p->subplan_, output, indent_level); // Print child at same level or indent_level+1?
                                                          // Usually, wrapper nodes don't add indent.
            break;
        }
        // Transaction plans
        case T_Transaction_begin: output += "TransactionBeginPlan\n"; break;
        case T_Transaction_commit: output += "TransactionCommitPlan\n"; break;
        case T_Transaction_abort: output += "TransactionAbortPlan\n"; break;
        case T_Transaction_rollback: output += "TransactionRollbackPlan\n"; break;
        
        default:
            output += "UnknownPlanNode(tag=" + std::to_string(plan->tag) + ")\n";
            // Attempt to print children if they are standard Unary/Binary (requires plan structure knowledge)
            // This part needs to be more robust if generic Unary/Binary plans exist
            // or if specific known plans with children aren't handled above.
            // For now, only known types with children are explicitly handled.
            break;
    }
}

// Helper to convert AggType to string (used by Aggregate node printing)
// This could be in common.h or here.
static std::string AggTypeStr(AggType type) {
    switch (type) {
        case AGG_COUNT: return "COUNT";
        case AGG_SUM: return "SUM";
        case AGG_MAX: return "MAX";
        case AGG_MIN: return "MIN";
        case AGG_COL: return "NONE"; // Should not happen for aggregated columns
        default: return "UNKNOWN_AGG";
    }
}
