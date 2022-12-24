import sys
import fire 
from stage1_board_search import Stage1
from stage2_board_url_scraping import Stage2 
from stage3_get_unique_pins import Stage3 
from stage4_download_images import Stage4
from typing import List
import os 
import getopt

def pintrest_scraper_cli(
                            search_term: str = None,
                            stages_to_execute: List[int] = [1, 2, 3, 4],
                            max_scrape_theads: int = 2,
                            max_pin_threads: int = 2,
                            use_proxy: bool = False,
                            ) -> None: 
    """Executes the chosen stages of the pintrest scraping, it raises error if stage 1 was chosen to be executed and `search_term` was not 
        a valid string.
    
    :param search_term: If stage 1 was chosen to be executed then it should be a valid string to search boards with that provided string. 
    :type search_term: str
    :param stages_to_execute: a list containing the number of stages required to be executed, default is a list containing all 4 stages `[1,2,3,4]`
    :type stages_to_execute: list
    :param maximum_scrape_theads: Maximum number of threads used in scraping the pins, default is `2` threads
    :type maximum_scrape_theads: int
    :param max_pin_threads: Maximum numbers of threads to scrap pins from board, default is `2` threads
    :type max_pin_threads: int
    :returns: 
    :rtype: None
    """
    #make sure that if stage1 is chosen `search_term` is a valid string
    if 1 in stages_to_execute: 
        assert isinstance(search_term, str)
    parsed_args = {}
    parsed_args["use_proxy"] = use_proxy
    parsed_args["max_pin_threads"] = max_pin_threads
    print(f"Args: {parsed_args}")
    #initialize instance of each stage.
    stages = {} 
    stages[1] = Stage1(parsed_args)        
    stages[2] = Stage2(parsed_args)
    #we can get rid of stage three since their won't be duplicated links        
    stages[3] = Stage3()        
    stages[4] = Stage4()     
    
    for stage_no in range(1, 5): 
        if stage_no in stages_to_execute: 
            
            if stage_no == 1: 
                stages[stage_no].run(search_term)
            elif stage_no == 4: 
                stages[stage_no].run(max_scrape_theads)
            else: 
                stages[stage_no].run()
            
    return 



if __name__ == "__main__": 
    
    fire.Fire(pintrest_scraper_cli)
    