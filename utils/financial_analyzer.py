# utils/financial_analyzer.py
"""Financial analysis and iXBRL Parser"""

from lxml import etree
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
from ..constants import (
    IXBRL_NAMESPACES,
    TAXONOMY_MAP
)
from .helpers import log_message


def create_secure_xml_parser():
    """
    Create a secure XML parser that prevents XXE (XML External Entity) attacks.
    
    This disables:
    - External entity resolution (prevents file disclosure)
    - Network access (prevents SSRF attacks)
    - DTD loading (prevents entity expansion attacks)
    """
    return etree.XMLParser(
        recover=True,
        huge_tree=True,
        resolve_entities=False,  # SECURITY: Prevent XXE file disclosure
        no_network=True,         # SECURITY: Prevent SSRF via external DTDs
        dtd_validation=False,    # Don't validate against DTD
        load_dtd=False,          # Don't load external DTD
    )


class iXBRLParser:
    def __init__(self, file_path):
        self.file_path = file_path
        # KEY FIX: Use secure XMLParser to prevent XXE attacks
        self.tree = etree.parse(file_path, create_secure_xml_parser())
        self.namespaces = IXBRL_NAMESPACES.copy()
        # Auto-detect actual namespaces from the file
        self._update_namespaces_from_file()
    
    def _update_namespaces_from_file(self):
        """Auto-detect and update namespaces from the actual file."""
        root = self.tree.getroot()
        nsmap = root.nsmap
        
        # Update our namespace dict with actual values from file
        for prefix, uri in nsmap.items():
            if prefix:  # Skip default namespace
                self.namespaces[prefix] = uri
    
    def _find_value(self, elements, tags_to_find, context_ref):
        """Find a value for any of the given tags with the specified context.

        Args:
            elements: Pre-computed list of ix:nonFraction elements (avoids
                      repeated full-tree traversal for every call).
            tags_to_find: List of XBRL tag local-names to search for.
            context_ref: The contextRef attribute value to match.
        """
        for tag in tags_to_find:
            for element in elements:
                name_attr = element.get('name', '')
                # Check if name ends with our tag (works regardless of prefix)
                if name_attr.endswith(f':{tag}') and element.get('contextRef') == context_ref:
                    if element.text:
                        try:
                            sign = element.get('sign', '')
                            value = float(element.text.strip().replace(',', ''))
                            if sign == '-':
                                value = -value
                            return value
                        except (ValueError, TypeError):
                            continue
        return None
    
    def parse_financials(self):
        """Parse financial data from iXBRL file."""
        financial_data = {}

        # Build the element list once — shared across all _find_value calls
        # to avoid repeating the full-tree traversal per metric per context.
        nf_elements = self.tree.findall('.//ix:nonFraction', namespaces=self.namespaces)

        # Find all context blocks
        contexts = self.tree.findall('.//xbrli:context', namespaces=self.namespaces)

        # Build a mapping of context_id -> year
        context_to_year = {}
        for context in contexts:
            context_id = context.get('id')
            instant_element = context.find('.//xbrli:instant', namespaces=self.namespaces)

            if context_id and instant_element is not None and instant_element.text:
                date_str = instant_element.text.strip()
                if '-' in date_str:
                    year = date_str.split('-')[0]
                    if year.isdigit():
                        context_to_year[context_id] = year

        # Group contexts by year (multiple context IDs can map to same year)
        year_to_contexts = {}
        for context_id, year in context_to_year.items():
            if year not in year_to_contexts:
                year_to_contexts[year] = []
            year_to_contexts[year].append(context_id)

        # Now find values using ALL contexts for each year
        for year, context_ids in year_to_contexts.items():
            if year not in financial_data:
                financial_data[year] = {}

            for key, tags in TAXONOMY_MAP.items():
                # Skip if already found
                if key in financial_data[year]:
                    continue

                # Try each context for this year
                for context_id in context_ids:
                    value = self._find_value(nf_elements, tags, context_id)
                    if value is not None:
                        financial_data[year][key] = value
                        break  # Found it, move to next metric

        return financial_data
    
    def get_all_available_tags(self):
        """Debug helper: Get all unique tag names used in the document."""
        elements = self.tree.findall('.//ix:nonFraction', namespaces=self.namespaces)
        tags = set()
        for elem in elements:
            name = elem.get('name')
            if name:
                tags.add(name)
        return sorted(tags)
    
    def get_all_contexts(self):
        """Debug helper: Get all context IDs and their dates."""
        contexts = self.tree.findall('.//xbrli:context', namespaces=self.namespaces)
        context_info = {}
        for context in contexts:
            context_id = context.get('id')
            instant = context.find('.//xbrli:instant', namespaces=self.namespaces)
            if context_id:
                context_info[context_id] = instant.text if instant is not None and instant.text else 'N/A'
        return context_info
    


class FinancialAnalyzer:
    """Handles multiple iXBRL files and performs financial analysis."""
    
    def __init__(self):
        self.data = pd.DataFrame()
        self.files_processed = []
    
    def load_files(self, file_paths: List[str]) -> pd.DataFrame:
        """Load multiple iXBRL files and combine into a single DataFrame."""
        all_data = []
        
        for file_path in file_paths:
            try:
                parser = iXBRLParser(file_path)
                financial_data = parser.parse_financials()
                
                # Extract filing year from filename or use latest year in data
                file_year = self._extract_filing_year(Path(file_path).name, financial_data)
                
                # Convert nested dict to list of records
                for year, metrics in financial_data.items():
                    record = {
                        'Year': int(year), 
                        'Source_File': Path(file_path).name,
                        'Filing_Year': file_year,
                        'Data_Completeness': len(metrics)  # Number of metrics found
                    }
                    record.update(metrics)
                    all_data.append(record)
                
                self.files_processed.append(file_path)
                log_message(f"✓ Processed: {Path(file_path).name} (Filing year: {file_year})")

            except Exception as e:
                log_message(f"✗ Error processing {Path(file_path).name}: {e}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            
            # Deduplicate: Keep the record from the most recent filing with most complete data
            log_message(f"→ Found {len(df)} total year records before deduplication")
            
            # Sort by Filing_Year (desc) and Data_Completeness (desc) to prioritize recent, complete data
            df = df.sort_values(['Year', 'Filing_Year', 'Data_Completeness'], 
                               ascending=[True, False, False])
            
            # Keep first occurrence of each year (most recent filing with most data)
            df_deduped = df.groupby('Year', as_index=False).first()
            
            duplicates_removed = len(df) - len(df_deduped)
            if duplicates_removed > 0:
                log_message(f"→ Removed {duplicates_removed} duplicate year record(s)")
                log_message(f"→ Retained {len(df_deduped)} unique year(s)")
            
            # Clean up and sort
            self.data = df_deduped.sort_values('Year').reset_index(drop=True)
            self.data['Year_Str'] = self.data['Year'].astype(str)
        
        return self.data
    
    def _extract_filing_year(self, filename: str, financial_data: dict) -> int:
        """Extract or infer the filing year from filename or data."""
        # Try to extract year from filename (e.g., "accounts_2021.xhtml" or "2021_accounts.xhtml")
        year_match = re.search(r'(20\d{2})', filename)
        if year_match:
            return int(year_match.group(1))
        
        # Fallback: use the most recent year in the financial data
        if financial_data:
            years = [int(y) for y in financial_data.keys() if y.isdigit()]
            if years:
                return max(years)
        
        # Last resort: current year
        return datetime.now().year
    
    def load_directory(self, directory_path: str, pattern: str = "*.xhtml") -> pd.DataFrame:
        """Load all iXBRL files from a directory."""
        dir_path = Path(directory_path)
        files = list(dir_path.glob(pattern))
        
        if not files:
            log_message(f"No files found matching pattern '{pattern}' in {directory_path}")
            return pd.DataFrame()

        log_message(f"Found {len(files)} files to process...")
        return self.load_files([str(f) for f in files])
    
    def summary(self) -> pd.DataFrame:
        """Get a summary view of the financial data."""
        if self.data.empty:
            log_message("No data loaded yet.")
            return pd.DataFrame()
        
        # Drop metadata columns for cleaner view
        cols_to_drop = ['Source_File', 'Filing_Year', 'Data_Completeness', 'Year_Str']
        return self.data.drop(columns=cols_to_drop, errors='ignore')
    
    def data_provenance(self) -> pd.DataFrame:
        """Show which file each year's data came from (after deduplication)."""
        if self.data.empty:
            log_message("No data loaded yet.")
            return pd.DataFrame()
        
        cols = ['Year', 'Source_File', 'Filing_Year', 'Data_Completeness']
        return self.data[[col for col in cols if col in self.data.columns]]
    
    def calculate_ratios(self) -> pd.DataFrame:
        """Calculate common financial ratios."""
        if self.data.empty:
            return pd.DataFrame()
        
        df = self.data.copy()
        
        # Current Ratio - with division by zero protection
        if 'CurrentAssets' in df.columns and 'CurrentLiabilities' in df.columns:
            df['CurrentRatio'] = df.apply(
                lambda row: row['CurrentAssets'] / row['CurrentLiabilities'] 
                if pd.notna(row['CurrentLiabilities']) and row['CurrentLiabilities'] != 0 
                else None, 
                axis=1
            )
        
        # Quick Ratio (assuming cash and debtors are liquid) - with division by zero protection
        if all(col in df.columns for col in ['CashBankInHand', 'Debtors', 'CurrentLiabilities']):
            df['QuickRatio'] = df.apply(
                lambda row: (row['CashBankInHand'] + row['Debtors']) / row['CurrentLiabilities']
                if pd.notna(row['CurrentLiabilities']) and row['CurrentLiabilities'] != 0
                else None,
                axis=1
            )
        
        # Return on Assets - with division by zero protection
        if 'ProfitLoss' in df.columns and 'TotalAssets' in df.columns:
            df['ROA'] = df.apply(
                lambda row: (row['ProfitLoss'] / row['TotalAssets']) * 100
                if pd.notna(row['TotalAssets']) and row['TotalAssets'] != 0
                else None,
                axis=1
            )
        
        # Profit Margin - with division by zero protection
        if 'ProfitLoss' in df.columns and 'Revenue' in df.columns:
            df['ProfitMargin'] = df.apply(
                lambda row: (row['ProfitLoss'] / row['Revenue']) * 100
                if pd.notna(row['Revenue']) and row['Revenue'] != 0
                else None,
                axis=1
            )
        
        # Debt to Equity (using retained earnings as proxy for equity) - with division by zero protection
        if 'CurrentLiabilities' in df.columns and 'NetAssets' in df.columns:
            df['DebtToEquity'] = df.apply(
                lambda row: row['CurrentLiabilities'] / row['NetAssets']
                if pd.notna(row['NetAssets']) and row['NetAssets'] != 0
                else None,
                axis=1
            )
        
        return df
    
    def year_over_year_growth(self) -> pd.DataFrame:
        """Calculate year-over-year growth rates."""
        if self.data.empty or len(self.data) < 2:
            log_message("Need at least 2 years of data for growth calculation.")
            return pd.DataFrame()
        
        df = self.data.copy()
        df = df.sort_values('Year')
        
        # Columns to calculate growth for
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        numeric_cols = [col for col in numeric_cols if col not in ['Year']]
        
        growth_data = []
        for i in range(1, len(df)):
            growth_record = {'Year': df.iloc[i]['Year']}
            for col in numeric_cols:
                if col in df.columns:
                    prev_val = df.iloc[i-1][col]
                    curr_val = df.iloc[i][col]
                    if pd.notna(prev_val) and pd.notna(curr_val) and prev_val != 0:
                        growth = ((curr_val - prev_val) / abs(prev_val)) * 100
                        growth_record[f'{col}_Growth_%'] = round(growth, 2)
            growth_data.append(growth_record)
        
        return pd.DataFrame(growth_data)
    
    def predict_next_year(self, metric: str, method: str = 'linear') -> Dict:
        """Simple prediction for next year using linear regression or average growth."""
        if self.data.empty or metric not in self.data.columns:
            return {}
        
        df = self.data[['Year', metric]].dropna().sort_values('Year')
        
        if len(df) < 2:
            return {}
        
        if method == 'linear':
            # Simple linear regression
            X = df['Year'].values
            y = df[metric].values
            
            # Calculate slope and intercept
            n = len(X)
            x_mean = X.mean()
            y_mean = y.mean()
            
            slope = sum((X - x_mean) * (y - y_mean)) / sum((X - x_mean) ** 2)
            intercept = y_mean - slope * x_mean
            
            next_year = X[-1] + 1
            prediction = slope * next_year + intercept
            
            return {
                'metric': metric,
                'next_year': int(next_year),
                'predicted_value': round(prediction, 2),
                'method': 'Linear Regression'
            }
        
        elif method == 'avg_growth':
            # Average growth rate
            growth_rates = []
            for i in range(1, len(df)):
                prev = df.iloc[i-1][metric]
                curr = df.iloc[i][metric]
                if prev != 0:
                    growth = (curr - prev) / abs(prev)
                    growth_rates.append(growth)
            
            if growth_rates:
                avg_growth = sum(growth_rates) / len(growth_rates)
                last_value = df.iloc[-1][metric]
                prediction = last_value * (1 + avg_growth)
                
                return {
                    'metric': metric,
                    'next_year': int(df.iloc[-1]['Year'] + 1),
                    'predicted_value': round(prediction, 2),
                    'avg_growth_rate': round(avg_growth * 100, 2),
                    'method': 'Average Growth Rate'
                }
        
        return {}
