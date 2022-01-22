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
    name = 'ideal_locations_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://www.ideal-locations.com/residential-lettings']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(.,'Read')]/@href").extract():
            follow_url = response.urljoin(item)
            if get_p_type_string(follow_url):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(follow_url)})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property/")[-1].split("-")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Ideal_Locations_PySpider_united_kingdom")
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        address = response.xpath("//h1[@class='property-location']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
     
        rent_string = response.xpath("//h1[@class='property-price']/small/text()").get()
        if rent_string: 
            if "pw" in rent_string:
                rent = rent_string.split("£")[-1].split("p")[0].replace(",","")
                item_loader.add_value("rent", str(int(rent.strip())*4))
                item_loader.add_value("currency","GBP")
            else:
                item_loader.add_value("rent_string", rent_string)       
   
        description = " ".join(response.xpath("//div[@class='property-description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
            
        coord = response.xpath("//script[contains(.,'mymap_lat=')]/text()").get()
        if coord:
            item_loader.add_value("latitude", coord.split("mymap_lat=")[1].split(";")[0])
            item_loader.add_value("longitude", coord.split("mymap_lng=")[1].split(";")[0])
      
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        room_count = response.xpath("//div[@class='feature_info']/text()[contains(.,'Bed')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bed")[0])
        bathroom_count = response.xpath("//div[@class='feature_info']/text()[contains(.,'Bath')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bath")[0])
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='slider']//li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
   
        item_loader.add_value("landlord_name", "Ideal Locations Ilford")
        item_loader.add_value("landlord_phone", "0208 518 1313")
        item_loader.add_value("landlord_email", "info@ideal-locations.com")
    
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