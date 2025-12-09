from typing import List, Dict, Any
from domain.document import Page
from docx import Document as DocxDocument

class VirtualPaginator:
    """
    Virtual page creation for DOCX documents
    This is still needed for proper page number metadata!
    """
    
    @staticmethod
    def create_virtual_pages(doc: DocxDocument, paragraphs_per_page: int = 50) -> List[Page]:
        """
        Create virtual pages for DOCX document
        Args:
            doc: DOCX document
            paragraphs_per_page: Number of paragraphs per virtual page
        Returns:
            List of Page objects
        """
        pages = []
        all_paragraphs = list(doc.paragraphs)
        
        for i in range(0, len(all_paragraphs), paragraphs_per_page):
            page_paragraphs = all_paragraphs[i:i + paragraphs_per_page]
            
            # Combine paragraph texts
            page_text = "\n".join([p.text for p in page_paragraphs if p.text.strip()])
            
            # Create page object
            page = Page(
                number=(i // paragraphs_per_page) + 1,
                raw_text=page_text,
                blocks=[
                    {
                        "text": p.text,
                        "style": p.style.name,
                        "type": "paragraph",
                        "line_number": j + 1
                    }
                    for j, p in enumerate(page_paragraphs)
                    if p.text.strip()
                ]
            )
            
            pages.append(page)
        
        return pages
    
    @staticmethod
    def calculate_lines_per_page(avg_chars_per_line: int = 80) -> int:
        """
        Calculate approximate lines per page based on character count
        """
        # Average page has about 2500-3000 characters
        avg_chars_per_page = 2750
        return avg_chars_per_page // avg_chars_per_line
    
    @staticmethod
    def estimate_page_count(paragraph_count: int, paragraphs_per_page: int = 50) -> int:
        """
        Estimate number of virtual pages needed
        """
        return max(1, (paragraph_count + paragraphs_per_page - 1) // paragraphs_per_page)
    
    @staticmethod
    def split_by_content_chunks(doc: DocxDocument, max_chars_per_page: int = 2750) -> List[Page]:
        """
        Create pages based on content size rather than paragraph count
        """
        pages = []
        current_page_content = []
        current_page_chars = 0
        page_number = 1
        
        for para in doc.paragraphs:
            para_text = para.text
            para_chars = len(para_text)
            
            # If adding this paragraph would exceed page limit, start new page
            if current_page_chars + para_chars > max_chars_per_page and current_page_content:
                # Save current page
                page_text = "\n".join([p.text for p in current_page_content])
                page = Page(
                    number=page_number,
                    raw_text=page_text,
                    blocks=[
                        {"text": p.text, "style": p.style.name, "type": "paragraph"}
                        for p in current_page_content
                    ]
                )
                pages.append(page)
                
                # Start new page
                current_page_content = [para]
                current_page_chars = para_chars
                page_number += 1
            else:
                current_page_content.append(para)
                current_page_chars += para_chars
        
        # Handle remaining content
        if current_page_content:
            page_text = "\n".join([p.text for p in current_page_content])
            page = Page(
                number=page_number,
                raw_text=page_text,
                blocks=[
                    {"text": p.text, "style": p.style.name, "type": "paragraph"}
                    for p in current_page_content
                ]
            )
            pages.append(page)
        
        return pages