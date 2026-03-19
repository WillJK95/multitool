# multitool/utils/fuzzy_match.py
"""
Shared fuzzy matching utilities for name and text comparison.

This module provides consistent fuzzy matching across all investigation modules,
using the same algorithm as the Unified Search module.
"""

import re
from typing import Tuple, Optional, List

# Import rapidfuzz - same library used in unified_search
try:
    from rapidfuzz.fuzz import WRatio
    FUZZY_AVAILABLE = True
except ImportError:
    WRatio = None
    FUZZY_AVAILABLE = False


def normalize_person_name(name: str) -> str:
    """
    Normalize a person's name for comparison.
    
    - Converts to lowercase
    - Removes titles (Mr, Mrs, Dr, etc.)
    - Handles "SURNAME, Forename" format
    - Removes punctuation
    - Normalizes whitespace
    
    Args:
        name: The name to normalize
        
    Returns:
        Normalized name string
    """
    if not name:
        return ""
    
    name = name.lower().strip()
    
    # Remove common titles
    titles = [
        r'\bmr\.?\s+', r'\bmrs\.?\s+', r'\bms\.?\s+', r'\bmiss\s+',
        r'\bdr\.?\s+', r'\bprof\.?\s+', r'\bprofessor\s+',
        r'\bsir\s+', r'\bdame\s+', r'\blord\s+', r'\blady\s+',
        r'\brev\.?\s+', r'\breverend\s+',
        r'\bhon\.?\s+', r'\bhonourable\s+',
        r'\brt\.?\s+hon\.?\s+', r'\bright\s+honourable\s+',
    ]
    for title_pattern in titles:
        name = re.sub(title_pattern, '', name, flags=re.IGNORECASE)
    
    # Handle "SURNAME, Forename" format -> "Forename SURNAME"
    if "," in name:
        parts = name.split(",", 1)
        if len(parts) == 2:
            name = f"{parts[1].strip()} {parts[0].strip()}"
    
    # Remove punctuation except spaces
    name = re.sub(r'[^\w\s]', ' ', name)
    
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name.strip()


def normalize_company_name(name: str) -> str:
    """
    Normalize a company name for comparison.
    
    Handles common variations:
    - "LIMITED" vs "LTD"
    - "PUBLIC LIMITED COMPANY" vs "PLC"
    - Removes punctuation
    
    Args:
        name: The company name to normalize
        
    Returns:
        Normalized company name
    """
    if not name:
        return ""
    
    name = name.lower().strip()
    
    # Standardize common suffixes
    replacements = [
        (r'\blimited\b', 'ltd'),
        (r'\bpublic limited company\b', 'plc'),
        (r'\bcommunity interest company\b', 'cic'),
        (r'\blimited liability partnership\b', 'llp'),
        (r'\bincorporated\b', 'inc'),
        (r'\bcorporation\b', 'corp'),
        (r'\bcompany\b', 'co'),
        (r'\b&\b', 'and'),
    ]
    for pattern, repl in replacements:
        name = re.sub(pattern, repl, name)
    
    # Remove punctuation
    name = re.sub(r'[^\w\s]', ' ', name)
    
    # Normalize whitespace
    name = ' '.join(name.split())
    
    return name.strip()


def fuzzy_score(text1: str, text2: str, normalize: bool = True) -> int:
    """
    Calculate fuzzy match score between two strings using WRatio.
    
    This is the same algorithm used in the Unified Search module.
    WRatio is particularly good for matching names as it handles
    word order differences and partial matches well.
    
    Args:
        text1: First string to compare
        text2: Second string to compare
        normalize: Whether to normalize strings before comparison
        
    Returns:
        Match score from 0 to 100
    """
    if not FUZZY_AVAILABLE:
        # Fallback to simple comparison
        if not text1 or not text2:
            return 0
        t1 = text1.lower().strip()
        t2 = text2.lower().strip()
        if t1 == t2:
            return 100
        elif t1 in t2 or t2 in t1:
            return 80
        return 0
    
    if not text1 or not text2:
        return 0
    
    if normalize:
        text1 = text1.lower().strip()
        text2 = text2.lower().strip()
    
    return int(WRatio(text1, text2))


def fuzzy_match_name(name1: str, name2: str) -> int:
    """
    Compare two person names using fuzzy matching.
    
    Normalizes names first (removes titles, handles surname-first format)
    then uses WRatio for comparison.
    
    Args:
        name1: First name to compare
        name2: Second name to compare
        
    Returns:
        Match score from 0 to 100
    """
    norm1 = normalize_person_name(name1)
    norm2 = normalize_person_name(name2)
    
    if not norm1 or not norm2:
        return 0
    
    return fuzzy_score(norm1, norm2, normalize=False)


def fuzzy_match_company(name1: str, name2: str) -> int:
    """
    Compare two company names using fuzzy matching.
    
    Normalizes company names first (standardizes LTD/LIMITED etc.)
    then uses WRatio for comparison.
    
    Args:
        name1: First company name
        name2: Second company name
        
    Returns:
        Match score from 0 to 100
    """
    norm1 = normalize_company_name(name1)
    norm2 = normalize_company_name(name2)
    
    if not norm1 or not norm2:
        return 0
    
    return fuzzy_score(norm1, norm2, normalize=False)


def generate_company_name_variants(name: str) -> set:
    """
    Generate common variations of a company name for matching.
    
    This matches the logic used in unified_search._match_company.
    
    Args:
        name: The company name
        
    Returns:
        Set of name variants (all lowercase)
    """
    if not name:
        return set()
    
    name_lower = name.lower()
    variants = {name_lower}
    
    # Handle Ltd / Limited
    if " limited" in name_lower:
        variants.add(name_lower.replace(" limited", " ltd"))
    elif " ltd" in name_lower:
        variants.add(name_lower.replace(" ltd", " limited"))
    
    # Handle PLC / Public Limited Company
    if " public limited company" in name_lower:
        variants.add(name_lower.replace(" public limited company", " plc"))
    elif " plc" in name_lower:
        variants.add(name_lower.replace(" plc", " public limited company"))
    
    # Handle CIC / Community Interest Company
    if " community interest company" in name_lower:
        variants.add(name_lower.replace(" community interest company", " cic"))
    elif " cic" in name_lower:
        variants.add(name_lower.replace(" cic", " community interest company"))
    
    # Handle LLP / Limited Liability Partnership
    if " limited liability partnership" in name_lower:
        variants.add(name_lower.replace(" limited liability partnership", " llp"))
    elif " llp" in name_lower:
        variants.add(name_lower.replace(" llp", " limited liability partnership"))
    
    return variants


def find_best_match(
    query: str,
    candidates: List[str],
    threshold: int = 85,
    scorer=None
) -> Tuple[Optional[str], int, Optional[int]]:
    """
    Find the best matching candidate for a query string.
    
    Uses the same approach as unified_search - iterates through all
    candidates and returns the single best match above the threshold.
    
    Args:
        query: The string to match
        candidates: List of candidate strings to match against
        threshold: Minimum score to consider a match (0-100)
        scorer: Custom scoring function (defaults to fuzzy_score)
        
    Returns:
        Tuple of (best matching candidate or None, score, index or None)
    """
    if not query or not candidates:
        return None, 0, None

    if scorer is None:
        scorer = fuzzy_score

    query_lower = query.lower()

    # Exact match always takes precedence — check first to short-circuit
    for idx, candidate in enumerate(candidates):
        if candidate and query_lower == candidate.lower():
            return candidate, 100, idx

    best_match = None
    best_score = 0
    best_index = None

    for idx, candidate in enumerate(candidates):
        if not candidate:
            continue

        score = scorer(query_lower, candidate.lower())

        if score > best_score:
            best_match = candidate
            best_score = score
            best_index = idx

    if best_match and best_score >= threshold:
        return best_match, best_score, best_index
    else:
        return None, best_score, None


def find_best_match_with_variants(
    query: str,
    candidates: List[str],
    threshold: int = 85
) -> Tuple[Optional[str], int, Optional[int]]:
    """
    Find the best match, trying multiple name variants.
    
    Generates variants of the query (e.g., "Ltd" -> "Limited")
    and finds the best match across all variants.
    
    This mirrors the approach in unified_search._match_company.
    
    Args:
        query: The string to match (typically a company name)
        candidates: List of candidate strings
        threshold: Minimum score to consider a match
        
    Returns:
        Tuple of (best matching candidate or None, score, index or None)
    """
    if not query or not candidates:
        return None, 0, None

    # Exact match always takes precedence — check first to short-circuit
    query_lower = query.lower()
    for idx, candidate in enumerate(candidates):
        if candidate and query_lower == candidate.lower():
            return candidate, 100, idx

    variants = generate_company_name_variants(query)

    best_match = None
    best_score = 0
    best_index = None

    for idx, candidate in enumerate(candidates):
        if not candidate:
            continue

        candidate_lower = candidate.lower()

        # Find the highest score for this candidate against any variant
        for variant in variants:
            score = fuzzy_score(variant, candidate_lower, normalize=False)
            if score > best_score:
                best_match = candidate
                best_score = score
                best_index = idx

    if best_match and best_score >= threshold:
        return best_match, best_score, best_index
    else:
        return None, best_score, None
