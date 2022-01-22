# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'park_estates'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):

        url="https://www.parkesestates.com/search-list/?instruction_type=Letting&showstc=on"
        yield Request(url, callback=self.parse)



    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@id='search-results']/div[@class='row']"):
            follow_url = response.urljoin(item.xpath("./div[@class='property']/div/a/@href").get())
            yield Request(follow_url, callback=self.populate_item)

            seen = True
        
        if page == 2 or seen:
            base_url = f"https://www.parkesestates.com/search-list/{page}.html?instruction_type=Letting&showstc=on"
            yield Request(
                base_url,
                callback=self.parse,
                meta={"page":page+1, "base_url":base_url}) 
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//h2/text()[contains(.,'Let Agreed')]").get()
        if rented:
            return
        # item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property-details/")[-1].split("/")[0])
        item_loader.add_value("external_source", "Park_estates_PySpider_united_kingdom")       
        item_loader.add_xpath("title", "//title/text()")

        description = " ".join(response.xpath("//div[@class='tab-content']//div[@class='col-md-12']//text()").getall())
        item_loader.add_value("description", re.sub("\s{2,}", " ", description))
         
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else:
            return
        address = " ".join(response.xpath("//div[@class='col-md-5']/h1/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='col-sm-3 col-md-3']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        rent = " ".join(response.xpath("//div[@class='col-md-5']/h2/text()").extract())
        if rent:
            price = rent.replace(",","").split("£")[1].strip().split(" ")[0].strip()
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "GBP")


        description = " ".join(response.xpath("//ul[@class='tick2']/li//text()").getall())
        if description:
            item_loader.add_value("description", re.sub("\s{2,}", " ", description))

        latlng = " ".join(response.xpath("//script/text()[contains(.,'latitude')]").getall())
        if latlng:
            item_loader.add_value("latitude", latlng.split('"latitude":')[1].split(",")[0].replace('"',"").strip())
            item_loader.add_value("longitude", latlng.split('"longitude":')[1].split("},")[0].replace('"',"").strip())

        item_loader.add_xpath("room_count", "//div[@class='room-icons']/span[*[@class='icon-bedrooms']]/strong/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='room-icons']/span[*[@class='icon-bathrooms']]/strong/text()")

        item_loader.add_value("landlord_name", "Parkes Estates Agents")       
        item_loader.add_value("landlord_phone", "+44 0207 368 6332")
        item_loader.add_value("landlord_email", "info@parkesestates.com")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None