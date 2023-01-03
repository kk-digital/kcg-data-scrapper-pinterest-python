import fire 
from stage1_board_search import Stage1
from stage2_board_url_scraping import Stage2 
from stage3_download_images import Stage3
from typing import List
import os 

def pintrest_scraper_cli(
                            search_term: str = None,
                            stages_to_execute: List[int] = [1, 2, 3, 4],
                            maximum_scrape_theads: int = 2
                            ) -> None: 
    """Executes the chosen stages of the pintrest scraping, it raises error if stage 1 was chosen to be executed and `search_term` was not 
        a valid string.
    
    :param search_term: If stage 1 was chosen to be executed then it should be a valid string to search boards with that provided string. 
    :type search_term: str
    :param stages_to_execute: a list containing the number of stages required to be executed, default is a list containing all 4 stages `[1,2,3,4]`
    :type stages_to_execute: list
    :param maximum_scrape_theads: Maximum number of threads used in scraping the pins, default is `2` threads
    :type maximum_scrape_theads: int
    :returns: 
    :rtype: None
    """
    #make sure that if stage1 is chosen `search_term` is a valid string
    if 1 in stages_to_execute: 
        assert isinstance(search_term, str)
    
    #initialize instance of each stage.
    stages = {} 
    stages[1] = Stage1(search_term)        
    stages[2] = Stage2(search_term)        
    stages[3] = Stage3(search_term)        
    
    for stage_no in range(1, 5):

        if stage_no in stages_to_execute: 
            
            print(f"[INFO] starting stage {stage_no}")
            
            if stage_no == 1: 
                stages[stage_no].run()
            elif stage_no == 4: 
                stages[stage_no].run(maximum_scrape_theads)
            else: 
                stages[stage_no].run()
            
    return 


if __name__ == "__main__": 
    fire.Fire(pintrest_scraper_cli)
    
