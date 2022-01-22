# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'uniplaces_com'
    execution_type='testing' 
    country='ireland'
    locale='en'
    external_source = "Uniplaces_PySpider_ireland"

    headers={
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "referer": "https://www.uniplaces.com/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"
    }

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=PT-porto&ne=41.19209228005314,-8.513226126530071&sw=41.1311692612593,-8.696304414695419&limit=48&key=vi4wif",
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=ES-barcelona&ne=41.512241123072066,2.3270002388813964&sw=41.306246554864885,1.961018183217334&limit=48&key=h891i9",               
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=ES-madrid&ne=40.536634441647216,-3.5365469065136494&sw=40.327571740373,-3.8794882000065627&limit=48&key=c31r5s",                
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=IT-milan&ne=45.56341709970796,9.359713245125022&sw=45.37079446183047,8.99373118946096&limit=48&key=k29f8o",               
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=IT-rome&ne=42.100151138365845,12.870102870008168&sw=41.6912633434104,12.14073270625454&limit=48&key=f9v1ir",                
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=DE-berlin&ne=52.685897155833125,13.727485804392927&sw=52.35163937446247,13.111564783885115&limit=48&key=vlyrdc",               
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=FR-paris&ne=48.96779628947335,2.7767291724818506&sw=48.60587457742426,1.9101825416224756&limit=48&key=sjqgvi", 
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=GB-london&ne=51.60637627003313,0.07369456700871524&sw=51.43547630048093,-0.30190052576472226&limit=48&key=1wg4z8",                
            },
            {
                "url" : "https://search-api-new.uniplaces.com/offers?city=PT-lisbon&ne=38.85592124946668,-9.058960916971728&sw=38.641716303602344,-9.423645998848542&limit=48&key=j1jh1z",               
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,headers=self.headers,
                                 meta={'domain_url':url.get('domain_url')})  

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data=json.loads(response.body)
        for url in data["data"]:
            follow_url=url["id"]
            location=response.url
            if "barcelona" in location:loc="barcelona"
            if "porto" in location:loc="porto"
            if "madrid" in location:loc="madrid"
            if "milan" in location:loc="milan"
            if "rome" in location:loc="rome"
            if "berlin" in location:loc="berlin"
            if "paris" in location:loc="paris"
            if "london" in location:loc="london"
            if "lisbon" in location:loc="lisbon"
            follow_url1=f"https://www.uniplaces.com/accommodation/{loc}/"+follow_url
            yield Request(follow_url1, callback=self.populate_item,meta={"item":url,"city":loc})
            seen = True
        if page == 2 or seen:  
            main_url=response.url.split("offers?")[-1]
            p_url = f"https://search-api-new.uniplaces.com/offers?page={page}&{main_url}"
            yield Request(
                p_url,
                callback=self.parse,
                dont_filter=True,headers=self.headers,
                meta={"page":page+1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item=response.meta.get("item")
        title=item["attributes"]["accommodation_offer"]["title"]
        if title:
            item_loader.add_value("title",title)
        rent=item["attributes"]["accommodation_offer"]["price"]["amount"]
        if rent:
            item_loader.add_value("rent",str(rent)[:3])
        item_loader.add_value("currency","EUR")
        available_date=item["attributes"]["accommodation_offer"]["available_from"]
        if available_date:
            item_loader.add_value("available_date",str(available_date).split("T")[0])
        city=response.meta.get("city")
        if city:
            item_loader.add_value("city",city)
        images=[f"https://cdn-static-new.uniplaces.com/property-photos/"+x["hash"]+"/small.jpg" for x in item["attributes"]["photos"]]
        if images:
            item_loader.add_value("images",images)
        room_count=item["attributes"]["property"]["number_of_rooms"]
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=item["attributes"]["property"]["number_of_bathrooms"]
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        latitude=item["attributes"]["property"]["coordinates"]
        if latitude:
            item_loader.add_value("latitude",str(latitude).split(",")[0].replace("[",""))
        longitude=item["attributes"]["property"]["coordinates"]
        if longitude:
            item_loader.add_value("longitude",str(longitude).split(",")[1].replace("]",""))
        property_type=item["attributes"]["property"]["type"]
        if property_type:
            item_loader.add_value("property_type",property_type)
        

        yield item_loader.load_item()