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
    name = 'zoomre_com_au'     
    execution_type='testing'
    country='australia'
    locale='en' 
    external_source='Zoomre_Com_PySpider_australia'
    start_urls = ["https://www.zoomre.com.au/residential-rentals"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property_thumbnail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='col-md-6']/h2/text()").get()
        if status and "under application" in status.lower():
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property_id=")[-1].split("/")[0])
        p_type = "".join(response.xpath("//div[@class='property_content']/div/p//text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            return
        item_loader.add_value("external_source", self.external_source)      
        item_loader.add_xpath("title","//div[@class='heading-text']/h2//text()")
        room_count = response.xpath("//div[i[@class='fa fa-bed']]/span/text()").get() 
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        if "studio" in get_p_type_string(p_type): 
            item_loader.add_value("room_count", "1")
        item_loader.add_xpath("bathroom_count", "//div[i[@class='fa fa-bath']]/span/text()")

        rent = "".join(response.xpath("//div[@class='widget bg-white widget_agents']/h2/span/text()").get())
        if "pw" in rent:
            rent = rent.strip().split("$")[1].split("pw")[0].strip()
            item_loader.add_value("rent",int(float(rent) * 4))
        elif "-" in rent:
            rent = rent.strip().split("$")[1].split("-")[0].strip()
            item_loader.add_value("rent",int(float(rent) * 4))
        item_loader.add_value("currency", 'AUD')
        
        address = response.xpath("//div[@class='heading-text']/h2//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        description = " ".join(response.xpath("//div[@class='property_content']//p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        parking = response.xpath("//div[i[@class='fa fa-car']]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        balcony = response.xpath("//ul[@class='features']//a[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True) 
     
        furnished = response.xpath("//div[@class='property_content']//p//text()[contains(.,'furnished') or contains(.,'Furnished') ]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)  
        
        script_map = response.xpath("//script[contains(.,').setView([')]/text()").get()
        if script_map:
            latlng = script_map.split(").setView([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        images = [x for x in response.xpath("//div[contains(@class,'egl-property-image-slider')]/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_xpath("landlord_name", "//div[@class='agent_info']/h3//text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent_info']/p/a[contains(@href,'tel')]/text()")
        item_loader.add_value("landlord_email", "info@zoomre.com.au")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "unit" in p_type_string.lower() or "villa" in p_type_string.lower() or "town" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None