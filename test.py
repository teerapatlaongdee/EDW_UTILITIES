import os

def remove_empty_files_and_folders(path):
    # Remove empty files
    for root, dirs, files in os.walk(path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if os.path.getsize(file_path) == 0:
                os.remove(file_path)
                print(f"Removed empty file: {file_path}")

    # Remove empty subfolders
    for root, dirs, files in os.walk(path, topdown=False):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            if not os.listdir(dir_path):
                os.rmdir(dir_path)
                print(f"Removed empty folder: {dir_path}")

    # Check the root folder itself
    if not os.listdir(path):
        os.rmdir(path)
        print(f"Removed empty folder: {path}")

# Example usage
remove_empty_files_and_folders('C:/scb100690/Playground/test_repo/Generate_Deployment/output_folder/SI-2691_SR-36170_SR-36171_250114_1600')
