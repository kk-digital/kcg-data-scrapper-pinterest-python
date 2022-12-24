import sqlite3
import sys
import os


# creating output folder.
out_folder = 'outputs'
os.makedirs(out_folder, exist_ok=True)

file_out_path = os.path.join(out_folder, 'output_of_third_tool.csv')
DATABASE_PATH = "database.db"

def get_all_pins_url():
    returns = []
    cmd = "select pin_url from stage2"
    try:
        with sqlite3.connect(DATABASE_PATH) as conn:
            cursor = conn.execute(cmd)
            conn.commit()
            for i in cursor:
                returns.append(i[0])
    except:
        return get_all_pins_url()
    return returns

class Stage3: 
    def __init__(self) -> None:
        pass
    
    def run(self) -> None: 
        """
            function to execute the third stage of Pintrest scraping, it simply excludes any duplicated pin urls so that in the fourth and 
            the last stage only the unique pins are being downloaded. 
        """
        print("started stage 3")
        links_pin = get_all_pins_url()
        # process
        pin_set = {}
        for i in links_pin:
            pin_set[i] = None

        print("There are ", len(pin_set), "unique urls/pins.")

        # output
        with open(file_out_path, "w") as f:
            for pin in pin_set:
                f.write(str(pin))
                f.write("\n") 

        print("finished stage 3")
        return 

if __name__ == '__main__':
    try:
        for i in range(len(sys.argv)):
            if(sys.argv[i] == '-o'):
                file_out_path = sys.argv[i+1]
                break
    except:
        print("file_out_path is not set yet!")
    # input 
    links_pin = get_all_pins_url()
    
    # process
    pin_set = {}
    for i in links_pin:
        pin_set[i] = None

    print("There are ", len(pin_set), "unique urls/pins.")

    # output
    with open(file_out_path, "w") as f:
        for pin in pin_set:
            f.write(str(pin))
            f.write("\n") 