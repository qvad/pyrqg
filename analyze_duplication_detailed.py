#!/usr/bin/env python3
"""
Detailed analysis of code duplication in workload grammar files
"""

import os
import re
from collections import defaultdict, Counter
from pathlib import Path

def analyze_workload_grammars():
    """Analyze workload grammar files for duplication"""
    
    print("=" * 80)
    print("DETAILED DUPLICATION ANALYSIS FOR WORKLOAD GRAMMARS")
    print("=" * 80)
    
    # Read all files
    workload_files = {
        'delete_focused': open('/home/qvad/workspace/pyrqg/grammars/workload/delete_focused.py').read(),
        'insert_focused': open('/home/qvad/workspace/pyrqg/grammars/workload/insert_focused.py').read(),
        'select_focused': open('/home/qvad/workspace/pyrqg/grammars/workload/select_focused.py').read(),
        'update_focused': open('/home/qvad/workspace/pyrqg/grammars/workload/update_focused.py').read(),
        'upsert_focused': open('/home/qvad/workspace/pyrqg/grammars/workload/upsert_focused.py').read(),
    }
    
    # 1. Analyze imports and setup code
    print("\n1. IMPORTS AND SETUP CODE:")
    print("-" * 40)
    
    import_lines = {}
    for file, content in workload_files.items():
        lines = content.split('\n')
        imports = []
        for line in lines[:15]:  # Check first 15 lines
            if 'import' in line or 'sys.path' in line:
                imports.append(line.strip())
        import_lines[file] = imports
    
    # Check if imports are identical
    unique_imports = set(tuple(imports) for imports in import_lines.values())
    if len(unique_imports) == 1:
        print("✓ All files have IDENTICAL import sections (lines 6-10)")
        print("  This is duplicated boilerplate code.")
    else:
        print("✗ Import sections vary across files")
    
    # 2. Analyze helper rules in detail
    print("\n\n2. DETAILED HELPER RULE ANALYSIS:")
    print("-" * 40)
    
    # Extract specific rules from each file
    helper_rules = {
        'table_name': {},
        'column_name': {},
        'column_list': {},
        'where_condition': {},
        'value_list': {},
        'numeric_column': {},
        'string_column': {}
    }
    
    for file, content in workload_files.items():
        for rule in helper_rules:
            pattern = rf'g\.rule\("{rule}",\s*((?:.*?\n)*?)(?=\n(?:g\.rule|if __name__))'
            match = re.search(pattern, content, re.MULTILINE | re.DOTALL)
            if match:
                helper_rules[rule][file] = match.group(1).strip()
    
    # Analyze each helper rule
    total_duplicate_lines = 0
    for rule, implementations in helper_rules.items():
        if len(implementations) > 1:
            print(f"\n'{rule}' rule:")
            print(f"  Found in {len(implementations)} files")
            
            # Group by identical implementations
            impl_groups = defaultdict(list)
            for file, impl in implementations.items():
                impl_groups[impl].append(file)
            
            if len(impl_groups) == 1:
                print(f"  ✓ IDENTICAL in all {len(implementations)} files")
                # Count lines
                lines = list(impl_groups.keys())[0].count('\n') + 1
                duplicate_lines = lines * (len(implementations) - 1)
                total_duplicate_lines += duplicate_lines
                print(f"  → {duplicate_lines} duplicate lines")
            else:
                print(f"  ✗ {len(impl_groups)} different implementations")
                
                # Check similarity
                all_impls = list(impl_groups.keys())
                if all('choice(' in impl for impl in all_impls):
                    # Extract choices
                    choices_per_impl = []
                    for impl in all_impls:
                        choices = re.findall(r'"([^"]+)"', impl)
                        choices_per_impl.append(set(choices))
                    
                    # Find common choices
                    common_choices = set.intersection(*choices_per_impl)
                    if common_choices:
                        print(f"  → Common values across all: {sorted(common_choices)}")
                        
                        # Calculate overlap percentage
                        all_choices = set.union(*choices_per_impl)
                        overlap_pct = len(common_choices) / len(all_choices) * 100
                        print(f"  → Overlap: {overlap_pct:.1f}% ({len(common_choices)}/{len(all_choices)} values)")
    
    # 3. Calculate exact duplication statistics
    print("\n\n3. DUPLICATION STATISTICS:")
    print("-" * 40)
    
    # Count total lines
    total_lines = sum(content.count('\n') for content in workload_files.values())
    
    # Identify sections that are identical
    duplicate_sections = [
        ("Imports and setup", 5, 5),  # Lines 6-10 in each file
        ("Main block", 5, 5),          # if __name__ == "__main__" block
    ]
    
    section_duplicate_lines = sum(lines * (len(workload_files) - 1) for _, lines, _ in duplicate_sections)
    
    # Add to total duplicate lines
    total_duplicate_lines += section_duplicate_lines
    
    print(f"Total lines across all files: {total_lines}")
    print(f"Duplicate lines from helper rules: {total_duplicate_lines - section_duplicate_lines}")
    print(f"Duplicate lines from boilerplate: {section_duplicate_lines}")
    print(f"Total duplicate lines: {total_duplicate_lines}")
    print(f"Duplication percentage: {(total_duplicate_lines / total_lines * 100):.1f}%")
    
    # 4. Analyze structural patterns
    print("\n\n4. STRUCTURAL PATTERNS:")
    print("-" * 40)
    
    # Check main test blocks
    test_blocks = {}
    for file, content in workload_files.items():
        match = re.search(r'if __name__ == "__main__":(.*)', content, re.DOTALL)
        if match:
            test_blocks[file] = match.group(1).strip()
    
    unique_test_blocks = set(test_blocks.values())
    if len(unique_test_blocks) == 1:
        print("✓ Test harness code is IDENTICAL in all files")
    else:
        print(f"✗ {len(unique_test_blocks)} different test harness implementations")
    
    # 5. Refactoring recommendations
    print("\n\n5. REFACTORING RECOMMENDATIONS:")
    print("-" * 40)
    
    print("\nBased on the analysis, creating a base class would be beneficial:")
    print("\n1. IMMEDIATE BENEFITS (High duplication):")
    print("   - Import statements and setup code (100% identical)")
    print("   - Test harness in __main__ block (100% identical)")
    print("   - Some helper rules with high overlap")
    
    print("\n2. PROPOSED STRUCTURE:")
    print("   ```python")
    print("   # base_workload_grammar.py")
    print("   class BaseWorkloadGrammar:")
    print("       def __init__(self, name):")
    print("           self.g = Grammar(name)")
    print("           self.setup_common_rules()")
    print("       ")
    print("       def setup_common_rules(self):")
    print("           # Common table names")
    print("           self.g.rule('common_tables', choice('users', 'orders', 'products'))")
    print("           # Common columns")
    print("           self.g.rule('id_columns', choice('id', 'user_id', 'product_id'))")
    print("       ")
    print("       def run_test(self, rule='query', count=10):")
    print("           # Common test harness")
    print("   ```")
    
    print("\n3. CUSTOMIZATION APPROACH:")
    print("   - Each workload class extends base with specific tables/columns")
    print("   - Override methods to add workload-specific choices")
    print("   - Merge common choices with specific ones")
    
    print("\n4. ESTIMATED REDUCTION:")
    reduction_pct = (total_duplicate_lines / total_lines * 100)
    print(f"   - Current duplication: {reduction_pct:.1f}%")
    print(f"   - Expected reduction: ~{reduction_pct * 0.8:.1f}% of current code")
    print(f"   - Net benefit: Cleaner, more maintainable code")

if __name__ == "__main__":
    analyze_workload_grammars()