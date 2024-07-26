import pickle

def create_pickle(object_name, file_name: str, path: str) -> None:
    """
    Creates a pickle file for object. Note: Path should have no slash 
    at the end
    """
    with open(path + f"/{file_name}", "wb") as storing_output:
        pickle.dump(object_name, storing_output)
        storing_output.close()


def read_pickle(file_name: str, path: str) -> None:
    """
    Reads pickle file from specified path 
    """
    pickle_file = open(path + f"/{file_name}", "rb")
    output = pickle.load(pickle_file)
    pickle_file.close()
    return output


def find_match(item: str, mapping: dict) -> str:
    """
    Finds the match for the item in the mapping dictionary
    """

    if type(item) is str:
        fixed_item = item.strip().lower()
        if fixed_item in mapping.keys():
            return mapping[fixed_item]
        else:
            return "No match found"
    else:
        return "No match found"