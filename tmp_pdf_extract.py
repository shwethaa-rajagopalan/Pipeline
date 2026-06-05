import PyPDF2
from pathlib import Path
path = Path('2024TM93028.pdf')
with path.open('rb') as f:
    reader = PyPDF2.PdfReader(f)
    print('pages', len(reader.pages))
    for i, page in enumerate(reader.pages[:3]):
        text = page.extract_text() or ''
        print('--- PAGE', i+1, '---')
        print(text[:3000])
