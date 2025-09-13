"""Step 01 Wrapper: January 2025 Equity Bhavcopy Download
Delegates to existing script nse_january_2025_downloader.py
"""
import importlib

def main():
    mod = importlib.import_module('nse_january_2025_downloader')
    if hasattr(mod, 'main'):
        return mod.main()

if __name__ == '__main__':
    main()
