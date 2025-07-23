#!/usr/bin/env python3
"""
Analyze code duplication in workload grammar files
"""

import os
import re
from collections import defaultdict
from pathlib import Path

def extract_rules(file_path):
    """Extract rule definitions from a grammar file"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find all g.rule() definitions
    rule_pattern = r'g\.rule\("([^"]+)",\s*((?:.*?\n)*?)(?=\n(?:g\.rule|if __name__))'
    rules = re.findall(rule_pattern, content, re.MULTILINE | re.DOTALL)
    
    return {name: definition.strip() for name, definition in rules}

def analyze_duplication():
    workload_dir = Path("/home/qvad/workspace/pyrqg/grammars/workload")
    files = [f for f in workload_dir.glob("*.py") if not f.name.endswith(".bak")]
    
    # Collect all rules from all files
    all_rules = {}
    file_rules = {}
    
    for file in files:
        file_name = file.stem
        rules = extract_rules(file)
        file_rules[file_name] = rules
        
        for rule_name, rule_def in rules.items():
            if rule_name not in all_rules:
                all_rules[rule_name] = {}
            all_rules[rule_name][file_name] = rule_def
    
    # Analyze duplicates
    print("=" * 80)
    print("DUPLICATION ANALYSIS FOR WORKLOAD GRAMMARS")
    print("=" * 80)
    
    # 1. Exact duplicate rules across files
    print("\n1. EXACT DUPLICATE RULES:")
    print("-" * 40)
    
    duplicate_count = 0
    total_rules = sum(len(rules) for rules in file_rules.values())
    
    for rule_name, definitions in all_rules.items():
        if len(definitions) > 1:
            # Check if all definitions are identical
            unique_defs = set(definitions.values())
            if len(unique_defs) == 1:
                duplicate_count += len(definitions) - 1
                print(f"\nRule '{rule_name}' is identical in:")
                for file in definitions.keys():
                    print(f"  - {file}.py")
                print(f"Definition: {list(unique_defs)[0][:100]}...")
    
    # 2. Similar rules (same name, different content)
    print("\n\n2. SIMILAR RULES (same name, different implementation):")
    print("-" * 40)
    
    for rule_name, definitions in all_rules.items():
        if len(definitions) > 1:
            unique_defs = set(definitions.values())
            if len(unique_defs) > 1:
                print(f"\nRule '{rule_name}' appears in {len(definitions)} files with {len(unique_defs)} different implementations:")
                for file, def_content in definitions.items():
                    print(f"  - {file}.py: {def_content[:80]}...")
    
    # 3. Helper rules analysis
    print("\n\n3. HELPER RULES ANALYSIS:")
    print("-" * 40)
    
    helper_rules = ["table_name", "column_name", "column_list", "where_condition", 
                    "value_list", "numeric_column", "string_column"]
    
    for rule in helper_rules:
        if rule in all_rules:
            files_with_rule = list(all_rules[rule].keys())
            if len(files_with_rule) > 1:
                print(f"\n'{rule}' appears in {len(files_with_rule)} files:")
                
                # Check for identical implementations
                unique_impls = {}
                for file, impl in all_rules[rule].items():
                    if impl not in unique_impls:
                        unique_impls[impl] = []
                    unique_impls[impl].append(file)
                
                if len(unique_impls) == 1:
                    print("  - All implementations are IDENTICAL")
                else:
                    print(f"  - {len(unique_impls)} different implementations found")
                    for impl, files in unique_impls.items():
                        print(f"    Files: {', '.join(files)}")
    
    # 4. Statistics
    print("\n\n4. STATISTICS:")
    print("-" * 40)
    
    # Count lines of code per file
    total_lines = 0
    duplicate_lines = 0
    
    for file in files:
        with open(str(file), 'r') as f:
            lines = f.readlines()
            total_lines += len(lines)
    
    # Estimate duplicate lines (rough approximation)
    for rule_name, definitions in all_rules.items():
        if len(definitions) > 1:
            unique_defs = set(definitions.values())
            if len(unique_defs) == 1:
                # Count lines in the duplicate definition
                def_lines = definitions[list(definitions.keys())[0]].count('\n') + 1
                duplicate_lines += def_lines * (len(definitions) - 1)
    
    duplication_percentage = (duplicate_lines / total_lines) * 100 if total_lines > 0 else 0
    
    print(f"Total files analyzed: {len(files)}")
    print(f"Total rules across all files: {total_rules}")
    print(f"Exact duplicate rule instances: {duplicate_count}")
    print(f"Total lines of code: {total_lines}")
    print(f"Estimated duplicate lines: {duplicate_lines}")
    print(f"Duplication percentage: {duplication_percentage:.1f}%")
    
    # 5. Refactoring recommendations
    print("\n\n5. REFACTORING RECOMMENDATIONS:")
    print("-" * 40)
    
    # Identify candidates for base class
    common_rules = []
    for rule_name, definitions in all_rules.items():
        if len(definitions) >= 3:  # Rule appears in 3+ files
            unique_defs = set(definitions.values())
            if len(unique_defs) == 1:  # All identical
                common_rules.append(rule_name)
    
    if common_rules:
        print("\nRules that should be moved to a base class (appear in 3+ files with identical implementation):")
        for rule in sorted(common_rules):
            print(f"  - {rule}")
    
    print("\n\nSUMMARY:")
    print("The workload grammars have moderate duplication, mainly in helper rules.")
    print("Creating a base class would be beneficial for:")
    print("  1. Common helper rules (table_name, column_name, etc.)")
    print("  2. Shared imports and setup code")
    print("  3. Common test harness code")

if __name__ == "__main__":
    analyze_duplication()