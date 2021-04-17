import os


class IOUtil:

    @staticmethod
    def create_dir_safely(dir_path: str):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)


    @staticmethod
    def delete_file_safely(file_path: str):
        if os.path.exists(file_path):
            os.remove(file_path)


    @staticmethod
    def get_file_size_in_MBs(file_path: str) -> float:
        size_in_bytes = 0
        if os.path.exists(file_path):
            size_in_bytes = os.path.getsize(file_path)
        else:
            raise ValueError("File at path: {} does not exist.".format(file_path))
        size_MBS = size_in_bytes / (1000 * 1000)
        return float("{0:.3f}".format(size_MBS))
