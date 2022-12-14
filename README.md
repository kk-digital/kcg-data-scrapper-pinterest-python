# Pinterest-scraper

## Tool Description
This tool can be used to make image scrapping from Pinterest, the tool have 4 stages:

<h4>Stage 1 - Board Search</h4>
Given a `search term` the crawler searches for boards using this term and stores the collected board links into a `sqlite` database to be used for collecting the pin urls in the second stage. 

<h4>Stage 2 - Board Url Scraping</h4>
Given the board urls stored in the database from `stage 1` the crawler go through those stored links and collects the pins links and stores them in the `sqlite` database to be used in scraping and downloading the pins images in `stage 4`. 

<h4>Stage 3 - Get Unique Pins </h4>
Before going to `stage 4` this stage is just simply excludes any duplicated pin urls so that in the fourth and the last stage, only the unique pins are being downloaded. 

<h4>Stage 4 - Download Images</h4>
Given the pin urls stored from `stage 2` and after the duplicated urls being excluded in `stage 3` this last stage is going through those pin links and downloading the images inside those pins, then compresses those downloaded images and uploading them to `Mega Upload`. 

## Requirements
This tool uses Chrome web driver ,afterwards run the following command to install the required dependencies for the tool. 
```sh
pip install -r ./requirements.txt
```

## Example Usages
* This command will execute all the 4 stages searching for `bears` images
```shell
python ./PinterestScraper.py --search_term='bears'
```


* This command will execute only the 3rd & 4th stages and will use the stored url links stored in the sqlite database
```shell
python ./PinterestScraper.py --stages_to_execute=[3,4]
```

## CLI Arguments and Options

* `search_term` _[string]_ - _[optional]_ - If stage 1 was chosen to be executed then it should be a valid string to search boards with that provided string or else the tool will raise error. 

* `stages_to_execute` _[list[int]]_ - _[optional]_ - a list containing the number of stages required to be executed, default is a list containing all 4 stages `[1,2,3,4]`

* `maximum_scrape_theads` _[int]_ - _[optional]_ Maximum number of threads used in scraping the pins, default is `2` threads

