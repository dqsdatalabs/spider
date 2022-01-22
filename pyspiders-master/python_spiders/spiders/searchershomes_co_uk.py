# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'searchershomes_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'       

    start_urls = ['https://www.searchershomes.co.uk/search?listingType=6&category=1&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='port-title-cont']"):
            url = item.xpath("./a/@href").extract_first()
            room_count = item.xpath(".//div[@class='itemRooms']/span[i[@class='icon-bed']]/preceding-sibling::text()[1]").extract_first()
            yield Request(response.urljoin(url), callback=self.populate_item,meta={'room_count': room_count })

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        description = "".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return

        item_loader.add_value("external_source", "Searchershomes_Co_PySpider_united_kingdom")
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("zipcode", title.split(",")[-1].strip())
           
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
        room_count = response.meta.get('room_count')
        if room_count and room_count.strip() !='0':
            item_loader.add_value("room_count", room_count)
        elif get_p_type_string(description) == "studio" or get_p_type_string(description) =="room":
            item_loader.add_value("room_count", "1")
   
        description = " ".join(response.xpath("//div[@class='descriptionsColumn']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        rent = " ".join(response.xpath("//h2[@class='fdPropPrice']/div/text()").getall())
        if rent:
            rent = rent.strip().replace("£","").strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//section[contains(@class,'transactions')]//text()[contains(.,'Deposit')]").get()
        if deposit:
            rent_week = int(float(int(rent)/4))
            deposit = deposit.split("weeks")[0].strip().split(" ")[-1]
            deposit = int(deposit)*rent_week
            item_loader.add_value("deposit", deposit)

        parking = response.xpath("//li//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking",True)    
        dishwasher = response.xpath("//li//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)    
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'fl-twelveWide')]/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        furnished = response.xpath("//li[contains(.,'FURNISHED') or contains(.,'Furnished') or contains(.,' furnished')]//text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        item_loader.add_value("landlord_name", "SEARCHERS PROPERTY MANAGEMENT")
        item_loader.add_value("landlord_phone", "0121 702 2222")
        item_loader.add_value("landlord_email", "enquiries@searchershomes.co.uk")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower() or "semi detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None