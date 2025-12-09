#!/usr/bin/env python3
"""
Header Filtering Utility
Provides filtering functions for header detection based on various criteria
"""

import re
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, field

@dataclass
class ExactHeadingRule:
    """
    Rule for exact heading detection
    """
    heading_text: str
    level: int = 1
    case_sensitive: bool = False
    whole_word: bool = True  # Only match when heading is followed by newline or end
    
    def matches(self, text: str) -> bool:
        """
        Check if text contains this exact heading
        """
        search_text = text if self.case_sensitive else text.lower()
        target_text = self.heading_text if self.case_sensitive else self.heading_text.lower()
        
        if self.whole_word:
            # Match exact text followed by newline or end of string
            pattern = re.escape(target_text) + r'(?:\n|$)'
            return bool(re.search(pattern, search_text, re.MULTILINE))
        else:
            return target_text in search_text

@dataclass
class HeaderFilter:
    """
    Header filter configuration with various filtering criteria
    """
    include_words: List[str] = field(default_factory=list)      # Words that must be present
    exclude_words: List[str] = field(default_factory=list)      # Words that must NOT be present
    include_regex: Optional[str] = None                        # Regex that text must match
    exclude_regex: Optional[str] = None                        # Regex that text must NOT match
    min_length: Optional[int] = None                           # Minimum text length
    max_length: Optional[int] = None                           # Maximum text length
    starts_with: Optional[str] = None                          # Text must start with this
    ends_with: Optional[str] = None                            # Text must end with this
    contains_pattern: Optional[str] = None                     # Text must contain this pattern
    
    def should_include(self, text: str) -> bool:
        """
        Check if text should be included based on all filter criteria
        """
        text_lower = text.lower().strip()
        
        # Check minimum length
        if self.min_length and len(text) < self.min_length:
            return False
        
        # Check maximum length
        if self.max_length and len(text) > self.max_length:
            return False
        
        # Check starts with
        if self.starts_with and not text_lower.startswith(self.starts_with.lower()):
            return False
        
        # Check ends with
        if self.ends_with and not text_lower.endswith(self.ends_with.lower()):
            return False
        
        # Check include words (at least one must be present)
        if self.include_words:
            if not any(word.lower() in text_lower for word in self.include_words):
                return False
        
        # Check exclude words (none must be present)
        if self.exclude_words:
            if any(word.lower() in text_lower for word in self.exclude_words):
                return False
        
        # Check contains pattern
        if self.contains_pattern:
            if not re.search(self.contains_pattern, text_lower, re.IGNORECASE):
                return False
        
        # Check include regex
        if self.include_regex:
            if not re.search(self.include_regex, text, re.IGNORECASE):
                return False
        
        # Check exclude regex
        if self.exclude_regex:
            if re.search(self.exclude_regex, text, re.IGNORECASE):
                return False
        
        return True

@dataclass
class HeaderFilterGroup:
    """
    Group of header filters with logical operators
    """
    filters: List[HeaderFilter] = field(default_factory=list)
    operator: str = "AND"  # "AND" or "OR"
    
    def should_include(self, text: str) -> bool:
        """
        Check if text should be included based on all filters in group
        """
        if not self.filters:
            return True  # If no filters, include everything
        
        results = [f.should_include(text) for f in self.filters]
        
        if self.operator == "AND":
            return all(results)
        elif self.operator == "OR":
            return any(results)
        else:
            raise ValueError(f"Invalid operator: {self.operator}. Use 'AND' or 'OR'")
    
    def add_filter(self, filter_config: Dict[str, Any]):
        """
        Add a new filter to the group
        """
        new_filter = HeaderFilter(
            include_words=filter_config.get('include_words', []),
            exclude_words=filter_config.get('exclude_words', []),
            include_regex=filter_config.get('include_regex'),
            exclude_regex=filter_config.get('exclude_regex'),
            min_length=filter_config.get('min_length'),
            max_length=filter_config.get('max_length'),
            starts_with=filter_config.get('starts_with'),
            ends_with=filter_config.get('ends_with'),
            contains_pattern=filter_config.get('contains_pattern')
        )
        self.filters.append(new_filter)

class ExactHeadingDetector:
    """
    Detector for exact heading matches
    """
    def __init__(self, exact_rules: List[ExactHeadingRule] = None):
        self.exact_rules = exact_rules or []
    
    def add_rule(self, rule: ExactHeadingRule):
        """Add a new exact heading rule"""
        self.exact_rules.append(rule)
    
    def add_rule_from_text(self, heading_text: str, level: int = 1, case_sensitive: bool = False):
        """Add a rule from heading text"""
        rule = ExactHeadingRule(
            heading_text=heading_text,
            level=level,
            case_sensitive=case_sensitive
        )
        self.add_rule(rule)
    
    def detect_exact_headings(self, text: str) -> List[tuple]:
        """
        Detect exact heading matches in text
        Returns: List of (heading_text, level) tuples
        """
        matches = []
        
        for rule in self.exact_rules:
            if rule.matches(text):
                matches.append((rule.heading_text, rule.level))
        
        return matches
    
    def get_matching_rules(self, text: str) -> List[ExactHeadingRule]:
        """
        Get all rules that match the text
        """
        return [rule for rule in self.exact_rules if rule.matches(text)]

class HeaderFilterManager:
    """
    Manages multiple header filter groups and exact heading detection
    """
    def __init__(self):
        self.filter_groups: List[HeaderFilterGroup] = []
        self.exact_detector = ExactHeadingDetector()
    
    def add_filter_group(self, group: HeaderFilterGroup):
        """
        Add a filter group to the manager
        """
        self.filter_groups.append(group)
    
    def add_exact_rule(self, rule: ExactHeadingRule):
        """
        Add an exact heading rule
        """
        self.exact_detector.add_rule(rule)
    
    def add_exact_rule_from_text(self, heading_text: str, level: int = 1, case_sensitive: bool = False):
        """
        Add an exact heading rule from text
        """
        rule = ExactHeadingRule(
            heading_text=heading_text,
            level=level,
            case_sensitive=case_sensitive
        )
        self.exact_detector.add_rule(rule)
    
    def should_include(self, text: str, level: Optional[int] = None) -> bool:
        """
        Check if text should be included based on all filter groups
        """
        if not self.filter_groups:
            return True  # If no filters, include everything
        
        # Check against all groups - text must pass at least one group (OR between groups)
        return any(group.should_include(text) for group in self.filter_groups)
    
    def detect_exact_headings(self, text: str) -> List[tuple]:
        """
        Detect exact headings in text
        """
        return self.exact_detector.detect_exact_headings(text)
    
    def get_filter_stats(self) -> Dict[str, Any]:
        """
        Get statistics about configured filters
        """
        return {
            "total_groups": len(self.filter_groups),
            "total_filters": sum(len(group.filters) for group in self.filter_groups),
            "total_exact_rules": len(self.exact_detector.exact_rules),
            "filters_per_group": [len(group.filters) for group in self.filter_groups]
        }

def create_default_header_filters() -> HeaderFilterManager:
    """
    Create a default header filter manager with common configurations
    """
    manager = HeaderFilterManager()
    
    # Add a default group for common header patterns
    default_group = HeaderFilterGroup(operator="OR")
    
    # Filter 1: Include words that suggest headers
    include_filter = HeaderFilter(
        include_words=["chapter", "section", "part", "appendix", "introduction", "conclusion"],
        exclude_words=["see", "refer", "table", "figure", "page"]
    )
    default_group.filters.append(include_filter)
    
    # Filter 2: Pattern-based headers
    pattern_filter = HeaderFilter(
        include_regex=r'^\d+\.(\d+\.?)*\s+.*$',  # Numbered headers: 1., 1.1, 1.1.1
        min_length=3,
        max_length=100
    )
    default_group.filters.append(pattern_filter)
    
    # Filter 3: Markdown-style headers
    markdown_filter = HeaderFilter(
        include_regex=r'^#+\s+.*$',  # # Header, ## Header, etc.
        min_length=5
    )
    default_group.filters.append(markdown_filter)
    
    manager.add_filter_group(default_group)
    return manager

def apply_header_filters(text: str, filters: Union[HeaderFilter, HeaderFilterGroup, HeaderFilterManager]) -> bool:
    """
    Apply header filters to determine if text should be included
    """
    if isinstance(filters, HeaderFilter):
        return filters.should_include(text)
    elif isinstance(filters, HeaderFilterGroup):
        return filters.should_include(text)
    elif isinstance(filters, HeaderFilterManager):
        return filters.should_include(text)
    else:
        raise ValueError(f"Invalid filter type: {type(filters)}")