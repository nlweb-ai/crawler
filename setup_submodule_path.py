"""
Utility module to set up the NLWeb submodule path.
Import this module before importing any core modules to ensure the path is set up correctly.
"""

import os
import sys

def setup_nlweb_submodule_path():
    """
    Add the NLWeb submodule to the Python path so we can import directly from it.
    This function can be called multiple times safely - it won't add duplicate paths.
    """
    # Calculate the path to the submodule
    # This works whether called from the root directory or from the code subdirectory
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    
    # If we're in the code directory, go up one level
    if os.path.basename(current_file_dir) == 'code':
        project_root = os.path.dirname(current_file_dir)
    else:
        project_root = current_file_dir
    
    nlweb_submodule_path = os.path.join(project_root, 'nlweb-submodule', 'code', 'python')
    
    # Only add the path if it exists and isn't already in sys.path
    if os.path.exists(nlweb_submodule_path) and nlweb_submodule_path not in sys.path:
        sys.path.insert(0, nlweb_submodule_path)
        return True
    
    return False

# Automatically set up the path when this module is imported
setup_nlweb_submodule_path()
