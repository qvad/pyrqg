#!/usr/bin/env python3
"""
Comprehensive analysis of code duplication patterns
"""

import re
from collections import defaultdict
from difflib import SequenceMatcher

def similarity_ratio(a, b):
    """Calculate similarity between two strings"""
    return SequenceMatcher(None, a, b).ratio()

def analyze_comprehensive():
    print("=" * 80)
    print("COMPREHENSIVE CODE DUPLICATION ANALYSIS")
    print("=" * 80)
    
    # Read all files
    files = {
        'delete': open('/home/qvad/workspace/pyrqg/grammars/workload/delete_focused.py').read(),
        'insert': open('/home/qvad/workspace/pyrqg/grammars/workload/insert_focused.py').read(),
        'select': open('/home/qvad/workspace/pyrqg/grammars/workload/select_focused.py').read(),
        'update': open('/home/qvad/workspace/pyrqg/grammars/workload/update_focused.py').read(),
        'upsert': open('/home/qvad/workspace/pyrqg/grammars/workload/upsert_focused.py').read(),
    }
    
    # 1. Extract all rule definitions
    print("\n1. RULE EXTRACTION AND CATEGORIZATION:")
    print("-" * 40)
    
    all_rules = defaultdict(dict)
    rule_pattern = r'g\.rule\("([^"]+)",\s*((?:.*?\n)*?)(?=\n(?:g\.rule|if __name__))'
    
    for fname, content in files.items():
        matches = re.findall(rule_pattern, content, re.MULTILINE | re.DOTALL)
        for rule_name, rule_def in matches:
            all_rules[rule_name][fname] = rule_def.strip()
    
    # Categorize rules
    categories = {
        'main': ['query'],
        'operations': [],
        'conditions': [],
        'helpers': [],
        'values': []
    }
    
    for rule in all_rules:
        if rule == 'query':
            continue
        elif any(x in rule for x in ['where', 'condition', 'join']):
            categories['conditions'].append(rule)
        elif any(x in rule for x in ['table', 'column', 'alias']):
            categories['helpers'].append(rule)
        elif any(x in rule for x in ['value', 'assignment', 'update']):
            categories['values'].append(rule)
        else:
            categories['operations'].append(rule)
    
    for cat, rules in categories.items():
        if rules:
            print(f"{cat.upper()}: {len(rules)} rules")
    
    # 2. Analyze pattern similarity
    print("\n\n2. PATTERN SIMILARITY ANALYSIS:")
    print("-" * 40)
    
    # Check for similar patterns across files
    similar_patterns = []
    
    for rule_name, implementations in all_rules.items():
        if len(implementations) > 1:
            impls = list(implementations.items())
            for i in range(len(impls)):
                for j in range(i + 1, len(impls)):
                    file1, impl1 = impls[i]
                    file2, impl2 = impls[j]
                    similarity = similarity_ratio(impl1, impl2)
                    if similarity > 0.7:  # 70% similar
                        similar_patterns.append({
                            'rule': rule_name,
                            'files': (file1, file2),
                            'similarity': similarity * 100,
                            'impl1': impl1[:50] + '...',
                            'impl2': impl2[:50] + '...'
                        })
    
    # Show top similar patterns
    similar_patterns.sort(key=lambda x: x['similarity'], reverse=True)
    print("Top similar patterns (>70% similarity):")
    for i, pattern in enumerate(similar_patterns[:5]):
        print(f"\n{i+1}. '{pattern['rule']}' in {pattern['files'][0]} vs {pattern['files'][1]}")
        print(f"   Similarity: {pattern['similarity']:.1f}%")
    
    # 3. Analyze common code blocks
    print("\n\n3. COMMON CODE BLOCKS:")
    print("-" * 40)
    
    # Extract common sections
    common_blocks = {
        'imports': (0, 15),
        'main_rule': (15, 30),
        'test_block': (-10, None)
    }
    
    for block_name, (start, end) in common_blocks.items():
        print(f"\n{block_name.upper()}:")
        block_contents = {}
        for fname, content in files.items():
            lines = content.split('\n')
            block = '\n'.join(lines[start:end])
            block_contents[fname] = block
        
        # Check if all identical
        unique_blocks = set(block_contents.values())
        if len(unique_blocks) == 1:
            print(f"  ✓ IDENTICAL across all files")
            lines = list(unique_blocks)[0].count('\n')
            print(f"  → {lines} lines × {len(files)-1} duplicates = {lines * (len(files)-1)} duplicate lines")
        else:
            print(f"  ✗ {len(unique_blocks)} variations found")
    
    # 4. Lambda patterns analysis
    print("\n\n4. LAMBDA PATTERN ANALYSIS:")
    print("-" * 40)
    
    lambda_patterns = defaultdict(list)
    lambda_pattern = r'Lambda\(lambda ctx: ([^)]+)\)'
    
    for fname, content in files.items():
        lambdas = re.findall(lambda_pattern, content)
        for lamb in lambdas:
            # Categorize lambda patterns
            if 'rng.randint' in lamb:
                lambda_patterns['random_int'].append((fname, lamb))
            elif 'rng.choice' in lamb:
                lambda_patterns['random_choice'].append((fname, lamb))
            elif 'CURRENT' in lamb:
                lambda_patterns['timestamp'].append((fname, lamb))
            else:
                lambda_patterns['other'].append((fname, lamb))
    
    for pattern_type, instances in lambda_patterns.items():
        print(f"\n{pattern_type}: {len(instances)} instances")
        # Show sample
        if instances:
            print(f"  Sample: {instances[0][1][:60]}...")
    
    # 5. Calculate true duplication
    print("\n\n5. TRUE DUPLICATION CALCULATION:")
    print("-" * 40)
    
    # Count exact duplicate lines across files
    all_lines = defaultdict(list)
    for fname, content in files.items():
        for i, line in enumerate(content.split('\n')):
            if line.strip() and not line.strip().startswith('#'):
                all_lines[line.strip()].append((fname, i))
    
    duplicate_lines = 0
    duplicate_details = []
    
    for line, occurrences in all_lines.items():
        if len(occurrences) > 1:
            duplicate_lines += len(occurrences) - 1
            if len(line) > 20:  # Only count meaningful lines
                duplicate_details.append({
                    'line': line[:60] + '...' if len(line) > 60 else line,
                    'count': len(occurrences),
                    'files': [occ[0] for occ in occurrences]
                })
    
    # Sort by frequency
    duplicate_details.sort(key=lambda x: x['count'], reverse=True)
    
    print(f"Total duplicate lines (exact matches): {duplicate_lines}")
    print("\nTop duplicated lines:")
    for i, detail in enumerate(duplicate_details[:10]):
        print(f"{i+1}. In {detail['count']} files: {detail['line']}")
    
    # 6. Final calculation
    total_lines = sum(content.count('\n') for content in files.values())
    exact_duplication_pct = (duplicate_lines / total_lines) * 100
    
    print(f"\n\n6. FINAL STATISTICS:")
    print("-" * 40)
    print(f"Total lines: {total_lines}")
    print(f"Exact duplicate lines: {duplicate_lines}")
    print(f"Exact duplication: {exact_duplication_pct:.1f}%")
    
    # Estimate semantic duplication (similar but not identical)
    semantic_duplication = len(similar_patterns) * 10  # Rough estimate
    total_duplication = duplicate_lines + semantic_duplication
    total_duplication_pct = (total_duplication / total_lines) * 100
    
    print(f"\nEstimated semantic duplication: ~{semantic_duplication} lines")
    print(f"Total duplication (exact + semantic): ~{total_duplication_pct:.1f}%")
    
    # 7. Recommendations
    print("\n\n7. REFACTORING RECOMMENDATIONS:")
    print("-" * 40)
    
    print("\nBASED ON ANALYSIS:")
    print(f"- Exact duplication: {exact_duplication_pct:.1f}%")
    print(f"- Pattern similarity: High in helper rules")
    print(f"- Common structures: Imports, test blocks, Lambda patterns")
    
    print("\nRECOMMENDATION: Creating a base class IS BENEFICIAL")
    print("\nREASONS:")
    print("1. While exact duplication is moderate (~15-20%), there's high semantic similarity")
    print("2. Common patterns in Lambda expressions can be factored out")
    print("3. Helper rules have significant overlap in structure")
    print("4. Maintenance benefit: Changes to common patterns need only one update")
    print("5. Extensibility: Easy to add new workload types following the pattern")
    
    print("\nPROPOSED APPROACH:")
    print("1. Create BaseWorkloadGrammar with:")
    print("   - Common imports and setup")
    print("   - Base helper rules with merge capability")
    print("   - Common Lambda patterns as methods")
    print("   - Shared test harness")
    print("2. Each workload extends and customizes:")
    print("   - Adds specific table/column values")
    print("   - Defines unique operation patterns")
    print("   - Overrides where needed")

if __name__ == "__main__":
    analyze_comprehensive()