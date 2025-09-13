"""Step 04 Wrapper: February 2025 F&O UDiFF Downloads
Delegates to existing script nse_fo_udiff_real_endpoint_downloader.py (currently month hard-coded inside)
"""
import importlib

def main():
    mod = importlib.import_module('nse_fo_udiff_real_endpoint_downloader')
    if hasattr(mod, 'main'):
        return mod.main()

if __name__ == '__main__':
    main()
