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
import dateparser

class MySpider(Spider):
    name = 'therent_guru'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://www.therent.guru/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[contains(@class,'propertyfilters')]/li/div/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.jump)
    
    def jump(self, response):
        
        for item in response.xpath("//li[contains(@class,'propertyitem')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        item_loader.add_value("external_source","Therent_PySpider_united_kingdom")
        desc = "".join(response.xpath("//div[@id='tabs-desc']//text()").getall())
        if desc and ("apartment" in desc.lower() or "flat" in desc.lower() or "maisonette" in desc.lower()):
            item_loader.add_value("property_type", "apartment")
        elif desc and "house" in desc.lower():
             item_loader.add_value("property_type", "house")
        elif desc and "studio" in desc.lower():
             item_loader.add_value("property_type", "studio")
        elif desc and "student" in desc.lower():
             item_loader.add_value("property_type", "student_apartment")
        else:
            return
        item_loader.add_value("description", desc) 

        title = " ".join(response.xpath("//title/text()").getall())
        if title:
            item_loader.add_value("title", title.strip()) 

        room_count = " ".join(response.xpath("substring-before(//p[@class='description lead']/text(),'BED')").getall())
        if room_count:
            room_count = room_count.strip().split(" ")[-1]
        if room_count.isdigit():
            item_loader.add_value("room_count", room_count)
        else:
            if 'double bedroom' in desc.lower():
                room_count = re.search(r"(\d) double bedroom", desc.lower())
                if room_count:
                    item_loader.add_value("room_count", room_count.group(1))
            elif 'studio' in desc.lower():
                item_loader.add_value("room_count", "1")
            elif "bedroom" in desc.lower():
                room_count = desc.lower().split("bedroom")[0].strip().split(" ")[-1].strip().replace("double", "")
                if room_count and room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
            elif "double room" in desc.lower():
                room_count = desc.lower().split("double room")[0].strip().split(" ")[-1].strip()
                if room_count and room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
            elif "room" in desc.lower():
                room_count = desc.lower().split("room")[0].strip().split(" ")[-1].strip().replace("double", "")
                if room_count and room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
            
        
        if "floor" in desc.lower():
            floor_list = ["first","second","third","fourth","fifth","sixth","seventh","eighth","ninth","tenth","eleventh","twelfth"]
            floor = desc.lower().split("floor")[0].strip().split(" ")[-1].strip()
            if floor and floor in floor_list:
                floor_number = floor_list.index(floor) + 1
                item_loader.add_value("floor", str(floor_number))

        address = " ".join(response.xpath("//div/div[@class='content-info']/h3/text()").getall())
        if address:
            item_loader.add_value("address", address.strip()) 
            item_loader.add_value("zipcode", address.split(".")[1].strip()) 
            item_loader.add_value("city", address.split(".")[0].split(",")[1].strip()) 

        available_date=response.xpath("//div[@class='block-info']/div[@class='info-result']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        rent = "".join(response.xpath("//div[@class='block-info']/label[contains(.,'Weekly rent :')]/following-sibling::div/text()").getall())
        if rent:
                price = rent.split("£")[1].strip()
                item_loader.add_value("rent", str(int(float(price))*4))
        item_loader.add_value("currency", "GBP")


        images = [ x for x in response.xpath("//div[@class='slider-cont']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)


        furnished = response.xpath("//ul[contains(@class,'prop-amenities')]/li/text()").getall()
        if furnished:
            if "Unfurnished" in furnished:
                item_loader.add_value("furnished", False)
            elif "Furnished" in furnished:
                item_loader.add_value("furnished", True)
            if "Washing Machine" in furnished:
                item_loader.add_value("washing_machine", True)

        Latlng ="".join(response.xpath('//*[@id="col__"]/div[1]/script[2][contains(.,"LatLng")]/text()').getall()) 
        if Latlng:
            item_loader.add_xpath("latitude", "substring-before(substring-after(//*[@id='col__']/div[1]/script[2][contains(.,'LatLng')]/text(),'LatLng('),',')")
            item_loader.add_xpath("longitude", "substring-before(substring-after(//*[@id='col__']/div[1]/script[2][contains(.,'LatLng')]/text(),','),')')")



        item_loader.add_value("landlord_name", " The Rent Guru")
        item_loader.add_value("landlord_phone", "0333 444 0016")
        item_loader.add_value("landlord_email", "mail@therent.guru")

  

        
        

        yield item_loader.load_item()
