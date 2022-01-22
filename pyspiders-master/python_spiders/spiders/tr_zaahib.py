# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader 
import json
import re

class MySpider(Spider):
    name = 'tr_zaahib'
    execution_type='testing'
    country='turkey'
    locale='tr' 
    external_source="Zaahib_PySpider_turkey"
    custom_settings = {
        "HTTPCACHE_ENABLED": False
    }
    

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://d-tr.zaahib.com/ajax/modules/classifieds/search_listings.php?sf=yes&ListingCategory%5Bin%5D=154&PropertyType%5Bin%5D=139&keywords%5Bcontains%5D=&lang=en&page=1&version=4.8.0&buildtype=website&Status%5Bin%5D=2"
                ],  
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://d-tr.zaahib.com/ajax/modules/classifieds/search_listings.php?sf=yes&ListingCategory%5Bin%5D=154&PropertyType%5Bin%5D=138%2C141%2C163&keywords%5Bcontains%5D=&lang=en&page=1&version=4.8.0&buildtype=website&Status%5Bin%5D=2"
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://d-tr.zaahib.com/ajax/modules/classifieds/search_listings.php?sf=yes&ListingCategory%5Bin%5D=154&PropertyType%5Bin%5D=459&keywords%5Bcontains%5D=&lang=en&page=1&version=4.8.0&buildtype=website&Status%5Bin%5D=2",
                ],
                "property_type": "room"
            },
        ]  # LEVEL 1

        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get('page', 2)
        data = json.loads(response.body)
        seen = False
        if "listings" in data:
            for d in data["listings"]:
                base_url = f"https://d-tr.zaahib.com/view_listing/en/{d['sid']}-{d['TypeEn']}-{d['CategoryEn']}-in{d['StreetNameEn']}-street-{d['CityEn']}"
                base_url = base_url.replace(" ", "-")
                title=d['PropertyTitleEn']

                follow_url = f"https://d-tr.zaahib.com/ajax/modules/classifieds/display_listing_handler.php?version=4.8.0&buildtype=website&cacheversion=107&lang=en&listing_id={d['sid']}&_=1634129455986"
                if d["Sold"] !=0:
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type'), "data": d,"base_url":base_url,"title":title})
                seen = True
        #page:200
        if page == 2 or seen:
            if page < 40:
                print(page)
                url = response.url.replace(f"page={page-1}", f"page={page}")
                yield Request(url, callback=self.parse, meta={"page": page+1, "property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response): 
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.meta.get('base_url'))
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
            
        item = json.loads(response.body)["listing"]

        item_loader.add_value("external_id", item["sid"])

        title = item["PropertyTitleEn"]    
        if title == "":
            title =  item["TypeEn"]

        item_loader.add_value("title", title)
        
        zipcode = item["ZipCode"]
        if zipcode:
            item_loader.add_value("zipcode", str(zipcode))

        rent = float(item['RentYearly'])
        if rent:
            rent = str(rent/12)
            item_loader.add_value("rent", rent.split('.')[0])
        else:
            rent = item['Rent']
            rent = rent.split(".")[0]
            item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "TRY")

        square_meters = item["SquareMeter"]
        if square_meters != 0:
            item_loader.add_value("square_meters", square_meters)

        item_loader.add_value("address", f"{item['ProvinceEn']} {item['CityEn']}")
        item_loader.add_value("city", item["ProvinceEn"])
        item_loader.add_value("floor", str(item["NumberOfLevels"]))

        room_count = item["Beds"]
        if room_count !=0:
            item_loader.add_value("room_count", room_count)
        elif response.meta.get('property_type') == "room":
            item_loader.add_value("room_count", 1)
        elif item["Rooms"] != 0:
            item_loader.add_value("room_count", item["Rooms"])

        bathroom_count = item["Baths"]
        if bathroom_count !=0:
            item_loader.add_value("bathroom_count", bathroom_count)

        description = item["LocationTextEn"]
        if description:
            item_loader.add_value("description", description)

        parking = item["GarageSize"]
        if parking!='0':
            item_loader.add_value("parking", True)

        furnished = item["Furnished"]
        if furnished!='0':
            item_loader.add_value("furnished", True)

        swimming_pool = item["SwimmingPool"]
        if swimming_pool!='0':
            item_loader.add_value("swimming_pool", True)

        elevator = item["Elevator"]
        if elevator!='0':
            item_loader.add_value("elevator", True)

        images = [x for x in item["pictureURLs"]]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("latitude", item["Latitude"])
        item_loader.add_value("longitude", item["Longitude"])

        user = item["user"]
        item_loader.add_value("landlord_phone", re.sub("\s{2,}", " ", item["MobileNumber"]))
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            item_loader.add_value("landlord_phone", re.sub("\s{2,}", " ", item["PhoneNumber"]))
        
        item_loader.add_value("landlord_name",re.sub("\s{2,}", " ", f"{user['FirstNameEn']} {user['LastNameEn']}"))
        item_loader.add_value("landlord_email", user["email"])
        
        yield item_loader.load_item()
        