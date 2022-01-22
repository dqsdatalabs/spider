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
    name = 'balgoresproperty_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.balgoresproperty.co.uk/properties/lettings/status-available"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page",2)
        seen = False
        for item in response.xpath("//span[@class='default-button-text']/.."):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.balgoresproperty.co.uk/properties/lettings/status-available/page-{page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1}
            )  
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Balgoresproperty_Co_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")

        f_text = " ".join(response.xpath("//div[contains(@class,'property-description')]//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        address = "".join(response.xpath("//h6[@class='text-left property-address']/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-2].strip()
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)

        rent = "".join(response.xpath("substring-before(substring-after(//p[contains(@class,'property-price')]/text(),'('),' ')").getall())
        if rent:
            price = rent.replace(",",".").replace(".","")
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR")

        room_count = "".join(response.xpath("//div[contains(@class,'property-tab')]/ul/li[contains(.,'Bedroom')]/text()").getall())
        if room_count:
            room = room_count.split(" ")[0].strip()
            if room !="0":
                item_loader.add_value("room_count",room)

        bathroom_count = "".join(response.xpath("//div[contains(@class,'property-tab')]/ul/li[contains(.,'Bathroom')]/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(" ")[0].strip())

        description = " ".join(response.xpath("//div[contains(@class,'property-description')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [response.urljoin(x) for x in response.xpath("//div[@class='a-photo']/div//@data-bg-hr").extract()]
        if images is not None:
            item_loader.add_value("images", images)

        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='tabFloorplan']/a/img/@src").extract()]
        if images is not None:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        item_loader.add_xpath("latitude", "//div[@class='map property-show-map']//@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='map property-show-map']//@data-lng")

        item_loader.add_xpath("landlord_phone", "substring-after(//div[@class='pull-left details']/ul/li/a/@href[contains(.,'tel')],'//')")
        landlord_name = "".join(response.xpath("//li[strong[contains(.,'Lettings')]]//text()[2][normalize-space()]").getall())
        if landlord_name:
            item_loader.add_value("landlord_name",landlord_name.strip())
        else:
            item_loader.add_value("landlord_name","Balgores Property Group")
        
        item_loader.add_value("landlord_email", "lettings@balgoresproperty.com")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None