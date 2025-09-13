"""Step 01 Wrapper: February 2025 Equity Bhavcopy Download
Delegates to existing script nse_february_2025_downloader.py
"""
import importlib

def main():
    mod = importlib.import_module('nse_february_2025_downloader')
    if hasattr(mod, 'main'):
        return mod.main()
    # fallback execute if module-level code performs download

if __name__ == '__main__':
    main()
