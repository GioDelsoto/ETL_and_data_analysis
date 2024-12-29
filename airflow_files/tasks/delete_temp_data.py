import os

def delete_files_in_folder(folder_path):
    """
    Deletes all files inside the specified folder.
    """
    if not os.path.exists(folder_path):
        print(f"Folder '{folder_path}' does not exist.")
        return

    if not os.path.isdir(folder_path):
        print(f"'{folder_path}' is not a valid folder.")
        return

    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            else:
                print(f"Skipped (not a file): {file_path}")
        print("All files deleted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
