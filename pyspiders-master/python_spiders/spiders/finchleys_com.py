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
    name = 'finchleys_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    start_urls = ['https://www.finchleys.com/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'propertiesBox')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            yield Request(follow_url, callback=self.populate_item)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        item_loader.add_value("external_source", "Finchleys_PySpider_united_kingdom")
        item_loader.add_value("external_id",response.url.split("/")[-1])
        desc = "".join(response.xpath("//div[contains(@class,'tabShowHide')]//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return

        rent = " ".join(response.xpath("//div[@class='row']/h2[2]/div/text()").extract())
        if rent:
            price = rent.replace(",","").strip().split(" ")[0].strip()
            item_loader.add_value("rent_string", price)
        
        address = " ".join(response.xpath("//title/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='royalSlider rsDefault']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        room_count = " ".join(response.xpath("//div[@class='row']/div[@class='detailRoomsIcon'][i[@class='i-bedrooms']]/text()").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.strip())


        bathroom_count = " ".join(response.xpath("//div[@class='row']/div[@class='detailRoomsIcon'][i[@class='i-bathrooms']]/text()").extract())
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        description = " ".join(response.xpath("//div[@class='detailsTabs']/div/p/text()").getall())
        if description:
            item_loader.add_value("description", re.sub("\s{2,}", " ", description))

            
        item_loader.add_value("landlord_name", "FINCHLEY'S ESTATE AGENTS")       
        item_loader.add_value("landlord_phone", "0208 346 1180")
        item_loader.add_value("landlord_email", "info@finchleys.com")
        
        yield item_loader.load_item()



def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None