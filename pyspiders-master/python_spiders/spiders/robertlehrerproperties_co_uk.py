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
from word2number import w2n

class MySpider(Spider):
    name = 'robertlehrerproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'   
    start_urls = ["http://robertlehrerproperties.co.uk/search?listingType=6&statusids=1&obc=price&obd=Ascending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1"]


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='relative']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://robertlehrerproperties.co.uk/search?listingType=6&statusids=1&obc=price&obd=Ascending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1&page={page}"
            yield Request(p_url, callback=self.parse, meta = {"page": page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("/")[-1])
        
        item_loader.add_value("external_source", "Robertlehrerproperties_PySpider_"+ self.country + "_" + self.locale)

        desc = "".join(response.xpath("//div[@class='descriptionsColumn']/text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        else:
            return

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1[@class='fdPropName']//text()").get()
        if address:
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = "".join(response.xpath("//h2[@class='fdPropPrice']/div[contains(.,'£')]/text()").getall())
        if rent and "PW" in rent:
            price = rent.strip().split("£")[1].split(" ")[0].replace(",","")
            item_loader.add_value("rent", str(int(price)*4))
        item_loader.add_value("currency", "GBP")
        
        desc = "".join(response.xpath("//div[contains(@class,'descriptionsColumn')]//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))

            if "furnished" in desc.lower():
                item_loader.add_value("furnished", True)
        
        if "Sq Metre" in desc:
            square_meters = desc.split("Sq Metre")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        elif "sq m/2" in desc:
            square_meters = desc.split("sq m/2")[0].strip().split(" ")[-1].replace("(","")
            item_loader.add_value("square_meters", square_meters)
        elif "sq feet" in desc:
            square_meters = desc.split("sq feet")[0].strip().split(" ")[-1].replace("(","")
            sqm = str(int(int(square_meters)* 0.09290304))
            item_loader.add_value("square_meters", sqm)
            
        if "bedroom" in desc.lower():
            room_count = desc.lower().split("bedroom")[0].strip().split(" ")[-1].replace("-","")
            if "double" in room_count:
                try:
                    room = desc.lower().split("double bedroom")[0].replace("large","").strip().split(" ")[-1].replace("l","")
                    if room.isdigit():
                        item_loader.add_value("room_count", room)
                    else:
                        item_loader.add_value("room_count", w2n.word_to_num(room))
                except:
                    pass
            elif room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                try:
                    room_count = w2n.word_to_num(room_count)
                    item_loader.add_value("room_count", room_count)
                except:
                    pass
        elif "studio" in desc:
            item_loader.add_value("room_count", "1")
            
        images = [ x for x in response.xpath("//div[contains(@class,'royalSlider')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "ROBERT LEHRER PROPERTIES")
        item_loader.add_value("landlord_phone", "lettings@robertlehrerproperties.co.uk")
        item_loader.add_value("landlord_email", "0208 340 3005")
        
        yield item_loader.load_item()
