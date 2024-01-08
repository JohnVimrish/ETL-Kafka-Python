import os

def generate_directory_structure(root_path, file_extension=None):
    structure = ""
    for root, dirs, files in os.walk(root_path):
        if file_extension:
            files = [file for file in files if file.endswith(file_extension)]
        relative_path = os.path.relpath(root, root_path)
        depth = len(relative_path.split(os.path.sep))
        structure += f"{'    ' * (depth - 1)}{'├── ' if depth > 1 else ''}{os.path.basename(root)}/\n"
        for file in files:
            structure += f"{'    ' * depth}{'│   ' if depth > 1 else ''}{file}\n"
    return structure

def update_readme_with_structure(readme_path, structure):
    with open(readme_path, 'a', encoding='utf-8') as readme_file:
        readme_file.write("\n## Directory Structure\n\n```\n" + structure + "```\n")

if __name__ == "__main__":
    project_root = input("Enter the project root directory: ")
    readme_path = os.path.join(project_root, 'README.md')
 
    if os.path.exists(readme_path):
        structure = generate_directory_structure(project_root)
        print(structure)
        update_readme_with_structure(readme_path, structure)
        print(f"Directory structure added to {readme_path}")
    else:
        print(f"Error: README file not found at {readme_path}")