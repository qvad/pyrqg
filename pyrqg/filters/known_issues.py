"""
Known issues filter for PyRQG.
Filters out results that match known issue patterns.
"""

import re
from typing import Dict, List, Optional, Union, Pattern, Callable, Any
from dataclasses import dataclass
from pathlib import Path
import json


@dataclass
class FilterRule:
    """A single filter rule."""
    name: str
    pattern: Optional[Pattern] = None
    function: Optional[Callable[[str], bool]] = None
    enabled: bool = True
    description: str = ""


class KnownIssuesFilter:
    """Filter for known database issues."""
    
    def __init__(self):
        self.rules: Dict[str, FilterRule] = {}
        self._load_default_rules()
    
    def _load_default_rules(self):
        """Load default YugabyteDB known issue rules."""
        # Bug #21012: HashAggregate with Group Key and InitPlan followed by Limit
        self.add_regex_rule(
            name="bug21012",
            pattern=r"\bHashAggregate\b.*[\n \[\],\']*\bGroup Key:.*[\n \[\],\']*\bInitPlan\b.*[\n \[\],\']*->\s+Limit",
            description="YugabyteDB Bug #21012: HashAggregate with InitPlan and Limit",
            flags=re.DOTALL | re.MULTILINE
        )
        
        # Additional YugabyteDB patterns (currently commented out in original)
        self.add_regex_rule(
            name="no_yb_bitmapscans",
            pattern=r"Yugabyte.*Bitmap",
            description="YugabyteDB: Bitmap scans not supported",
            enabled=False  # Disabled by default like in original
        )
        
        self.add_function_rule(
            name="require_yb_bitmapscans",
            function=lambda text: "Yugabyte" in text and "Bitmap" not in text,
            description="YugabyteDB: Require bitmap scans",
            enabled=False
        )
        
        self.add_regex_rule(
            name="no_yb_bnl",
            pattern=r"Yugabyte.*YB Batched Nested Loop",
            description="YugabyteDB: Batched Nested Loop issues",
            enabled=False
        )
        
        self.add_function_rule(
            name="require_yb_bnl",
            function=lambda text: "Yugabyte" in text and "YB Batched Nested Loop" not in text,
            description="YugabyteDB: Require Batched Nested Loop",
            enabled=False
        )
    
    def add_regex_rule(self, name: str, pattern: str, description: str = "", 
                      enabled: bool = True, flags: int = 0):
        """Add a regex-based filter rule."""
        compiled_pattern = re.compile(pattern, flags)
        self.rules[name] = FilterRule(
            name=name,
            pattern=compiled_pattern,
            description=description,
            enabled=enabled
        )
    
    def add_function_rule(self, name: str, function: Callable[[str], bool], 
                         description: str = "", enabled: bool = True):
        """Add a function-based filter rule."""
        self.rules[name] = FilterRule(
            name=name,
            function=function,
            description=description,
            enabled=enabled
        )
    
    def matches_known_issue(self, text: str, check_all: bool = False) -> List[str]:
        """
        Check if text matches any known issue patterns.
        
        Args:
            text: Text to check (usually error message or query plan)
            check_all: If True, check all rules regardless of enabled status
            
        Returns:
            List of matched rule names
        """
        matched_rules = []
        
        for rule_name, rule in self.rules.items():
            if not check_all and not rule.enabled:
                continue
            
            if rule.pattern and rule.pattern.search(text):
                matched_rules.append(rule_name)
            elif rule.function and rule.function(text):
                matched_rules.append(rule_name)
        
        return matched_rules
    
    def should_filter(self, text: str) -> bool:
        """
        Check if text should be filtered out.
        
        Returns True if the text matches any enabled known issue.
        """
        return len(self.matches_known_issue(text)) > 0
    
    def load_rules_from_file(self, filepath: str):
        """Load filter rules from a file."""
        path = Path(filepath)
        
        if path.suffix == ".json":
            # Load from JSON format
            with open(path, 'r') as f:
                rules_data = json.load(f)
            
            for rule_name, rule_info in rules_data.items():
                if "pattern" in rule_info:
                    self.add_regex_rule(
                        name=rule_name,
                        pattern=rule_info["pattern"],
                        description=rule_info.get("description", ""),
                        enabled=rule_info.get("enabled", True),
                        flags=rule_info.get("flags", 0)
                    )
        elif path.suffix == ".ff":
            # Parse original .ff format
            self._parse_ff_file(filepath)
    
    def _parse_ff_file(self, filepath: str):
        """Parse original .ff filter file format."""
        with open(filepath, 'r') as f:
            content = f.read()
        
        # Extract rules using regex
        # Pattern: 'rule_name' => qr{pattern}flags,
        rule_pattern = r"'(\w+)'\s*=>\s*qr\{(.*?)\}(\w*)"
        
        for match in re.finditer(rule_pattern, content, re.DOTALL):
            rule_name = match.group(1)
            pattern = match.group(2)
            flags_str = match.group(3)
            
            # Convert Perl flags to Python
            flags = 0
            if 'o' in flags_str:
                pass  # 'o' is compile once, default in Python
            if 's' in flags_str:
                flags |= re.DOTALL
            if 'm' in flags_str:
                flags |= re.MULTILINE
            if 'i' in flags_str:
                flags |= re.IGNORECASE
            
            # Check if rule is commented out
            line_start = content.rfind('\n', 0, match.start()) + 1
            line = content[line_start:match.start()].strip()
            enabled = not line.startswith('#')
            
            self.add_regex_rule(
                name=rule_name,
                pattern=pattern,
                enabled=enabled,
                flags=flags
            )
    
    def get_enabled_rules(self) -> List[str]:
        """Get list of enabled rule names."""
        return [name for name, rule in self.rules.items() if rule.enabled]
    
    def get_all_rules(self) -> List[str]:
        """Get list of all rule names."""
        return list(self.rules.keys())
    
    def enable_rule(self, name: str):
        """Enable a specific rule."""
        if name in self.rules:
            self.rules[name].enabled = True
    
    def disable_rule(self, name: str):
        """Disable a specific rule."""
        if name in self.rules:
            self.rules[name].enabled = False
    
    def get_rule_info(self, name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific rule."""
        if name not in self.rules:
            return None
        
        rule = self.rules[name]
        return {
            "name": rule.name,
            "enabled": rule.enabled,
            "description": rule.description,
            "type": "regex" if rule.pattern else "function",
            "pattern": rule.pattern.pattern if rule.pattern else None
        }