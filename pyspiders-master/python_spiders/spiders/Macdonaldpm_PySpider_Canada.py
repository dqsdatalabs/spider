import re
import scrapy
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import requests
import datetime

class MacdonaldpmSpider(scrapy.Spider):
    name = 'macdonaldpm'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['macdonaldpm.com']
    start_urls  =   ["https://macdonaldpm.com/api/rental.json"]
    
    def parse(self, response):
        
        req = requests.get(response.url)
        data = json.loads(req.content)
        for items in data['listings']:
            request =  scrapy.Request(url="https://macdonaldpm.com/rentals/results/"+items['id']+"/"+items['slug'], callback=self.get_desc)
            
            item = ListingLoader(response=response)
            item.add_value("external_link"  , "https://macdonaldpm.com/rentals/results/"+items['id']+"/"+items['slug'])
            item.add_value("external_id"    , items['id'])
            item.add_value("external_source", self.external_source)
            item.add_value("images"         , items['gallery'])
            item.add_value("city"           , items['city']['city'])
            item.add_value("address"        , items['title'])
            item.add_value("room_count"     , int(re.findall("[0-9]+",items['beds'][0])[0]))
            item.add_value("bathroom_count" , int(float(items['baths'])))
            item.add_value("rent"           , int(items['price']))
            item.add_value("square_meters"  , int(int(sq_feet_to_meters(int(items['size'])))*10.764))
            item.add_value("currency"       , "CAD")
            item.add_value("property_type"  , make_property(items["propertyType"][0]))
            item.add_value("available_date" , items["date"]["date"].replace("00:00:00.000000","").strip())
            item.add_value("title"          , items["title"])

            request.meta['loader'] = item
            yield request
    
    def get_desc(self,response):
        pool,parking,pets,washing_machine = '','','',''
        f        = response.css(".listingIcons ul li span::text").getall()
        if 'Parking' in f:
            parking = True
        if 'Pool' in f:
            pool = True
        if 'Dogs' in f or 'Cats' in f:
            pets = True
        if 'Laundry' in f :
            washing_machine = True
        
        longitude, latitude = re.findall("-*[0-9]+\.*[0-9]*",response.css("script::text").re(r"smartMap.coords(.*)")[0])
        item = response.meta['loader']
        item.add_value('description'            ,remove_white_spaces(" ".join(response.css("#sidebarMove .description p::text").getall())))
        item.add_value('landlord_name'          ,response.css(".broker .details h4::text").get())
        item.add_value('landlord_phone'         ,remove_white_spaces(response.css(".broker .details .phone::text").get()))
        item.add_value('parking'                ,parking)
        item.add_value('swimming_pool'          ,pool)
        item.add_value('pets_allowed'           ,pets)
        item.add_value('longitude'              ,longitude)
        item.add_value('latitude'               ,latitude)
        item.add_value('washing_machine'        ,washing_machine)
        
        yield  item.load_item()
        




def make_property(prop):
    if prop in ['condo', 'basement-suite', 'duplex', 'suite']:
        return 'apartment'
    elif prop in ['townhouse']:
    	return 'house'
    return prop
