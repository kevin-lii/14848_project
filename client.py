import tarfile
import os
import time
import requests


def load_file(filenames):
    # Sends the file contents to the gcp server application
    for filename in filenames:
        file = os.path.abspath(filename)
        try:
            tar = tarfile.open(file, "r:gz")
            if (tar == None):
                raise FileNotFoundError(
                    "Aborting: No files selected. Please select a valid file.")
            for member in tar.getmembers():
                f = tar.extractfile(member)
                if f is not None:
                    content = f.read().decode("utf-8")
                    print(filename, type(content), content[:5])
            # response = requests.post(
            #     'https://[YOUR_PROJECT_ID].appspot.com/_ah/api/load', data={'content': content, 'filename': filename})
        except FileNotFoundError:
            print("Aborting: File '{}' not found. Make sure that you are choosing a valid file.".format(
                filename))
            # exit(1)


def top_n(val):
    response = requests.get(
        'https://[YOUR_PROJECT_ID].appspot.com/_ah/api/topn', params={'val': val})
    # Retrieves the top-n results from gcp server application and formats it to table form
    return None


def search_for_term(word):
    response = requests.get(
        'https://[YOUR_PROJECT_ID].appspot.com/_ah/api/search', params={'word': word})
    # Retrieves the search-from-term results from gcp server application and formats it to table form
    return None


if __name__ == "__main__":
    print("=========================")
    print("Welcome to Load My Engine")
    print("=========================\n")
    files = []
    while (True):
        val = input("Please select a tar.gz file you would like to process: ")
        files.append(val)
        exit_loop = False
        while(True):
            cont = input("Would you like to select another file? [y/N]: ")
            cont = cont.strip().lower()
            if cont == "y" or cont == "yes":
                break
            elif cont == "n" or cont == "no":
                exit_loop = True
                break
            else:
                print("Your choice was invalid. Please select either [y/N]")
        if exit_loop:
            break
    print("You have selected the follow files:")
    print(files)
    print("Loading your engine...")
    content = load_file(files)
    print("\n=========================")
    print("Engine was loaded!")
    print("=========================\n")
    while(True):
        print("\nYou can now either 'Search for Term' or view 'Top-N' terms")
        print("To exit the program, type 'e' or 'exit'")
        val = input("Please select action...[S/TN]: ")
        val = val.strip().lower()
        if val == 's' or val == "search" or val == "search for term":
            val = input("Enter Your Search Term: ")
            val = val.strip()
            start_time = time.time()
            table = search_for_term(val)
            print("\nYou searched for the term: {}".format(val))
            timeElapsed = time.time() - start_time
            print("Your search was executed in {} seconds".format(timeElapsed))
            print(table)
        elif val == 't' or val == "tn" or val == "topn" or val == "top-n":
            val = input("Enter Your N Value (must be an integer): ")
            val = val.strip()
            while not val.isnumeric():
                print(
                    "Error: The value you just entered was not an integer. Please try again")
                val = input("\nEnter Your N Value (must be an integer): ")
                val = val.strip()
            print("Top-{} Frequent Term(s)".format(val))
            table = top_n(val)
            print(table)
        elif val == 'e' or val == 'exit':
            print("\n=========================")
            print("Goodbye!")
            print("=========================\n")
            break
        else:
            print("You entered an invalid input. Please try again\n")
