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
    name = 'redstones_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ["https://www.redstones.co.uk/property-search/?radius=101&bedrooms=0&min-price=0&max-price=999999999999&tender=lettings&location="]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        total_page = int(response.xpath("//div[contains(@class,'rds-pagination')]/ul/li[last()]/a/text()").get())

        for item in response.xpath("//a[@class='rds-post__image']"):
            status = item.xpath("./div/h4/small/text()").get()
            if status and "agreed" in status.lower():
                continue
            f_url = response.urljoin(item.xpath("./@href").get())
            yield Request(f_url, self.populate_item)
        
        if page <= total_page:
            p_url = f"https://www.redstones.co.uk/property-search/page/{page}/?radius=101&bedrooms=0&min-price=0&max-price=999999999999&tender=lettings&location"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        title = " ".join(response.xpath("//div//h3[contains(@class,'text-primary')]//text()[normalize-space()]").extract())
        if not title:
            title = " ".join(response.xpath("//div[@class='col-md-8']/h1//text()").extract())
        if title:
            if "Serviced Office Suites" in title:
                return
            elif "Storage Containers" in title:
                return
            item_loader.add_value("title", title.replace("   "," ").strip()) 
        full_text = "".join(response.xpath("//div[contains(@class,'col-md-6 offset-lg-1')]//text()").getall())
        if get_p_type_string(full_text):
            item_loader.add_value("property_type", get_p_type_string(full_text))
        else:
            return
        item_loader.add_value("external_source", "Redstones_Co_PySpider_united_kingdom")
        
        rent = " ".join(response.xpath("//h2/text()").extract())
        if rent:
            if "Weekly" in rent:
                price = rent.replace("Weekly","").replace("£","").strip()
                item_loader.add_value("rent", int(price)*4) 
            else:
                item_loader.add_value("rent", rent.strip())
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//div//h3[contains(@class,'text-primary')]//text()[contains(.,'Bedroom')]").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split("Bed")[0])
            
        city = response.xpath("//div//h6//text()").extract_first()       
        if city:
            item_loader.add_value("zipcode", city.split(",")[-1].strip()) 
            item_loader.add_value("city", city.split(",")[-2].strip()) 

        address = response.xpath("//div[@class='col-md-8']/h1//text()").extract_first()       
        if address:
            item_loader.add_value("address", address.strip())  
        elif not address and city:
            item_loader.add_value("address", city.strip())  

        desc = " ".join(response.xpath("//div[contains(@class,'offset-lg-1')]//text()[not(contains(.,'Request a Viewing'))]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
                 
        floor = response.xpath("//div[@id='tab-features']//li//text()[contains(.,'Floor')]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.lower().split("floor")[0].strip().split(" ")[-1])
   
        parking = response.xpath("//div[@id='tab-features']//li//text()[contains(.,'Parking') or contains(.,'parking') or contains(.,'Car Park')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)

        elevator = response.xpath("//div[@id='tab-features']//li//text()[contains(.,'Lift') or contains(.,'lift')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//div[@id='tab-features']//li//text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        lat = response.xpath("//div[@class='marker']/@data-lat").get()
        lng = response.xpath("//div[@class='marker']//@data-lng").get()
        if lng and lng:
            item_loader.add_value("latitude",lat.strip())
            item_loader.add_value("longitude",lng.strip())
          
        images = []
        img = response.xpath("//div[@class='rds-slick--property']/div/@style").extract()
        if img:
            for j in img:            
                image = j.split("image:url(")[1].split(")")[0]
                images.append(response.urljoin(image))
            if images:
                item_loader.add_value("images", images)  

        item_loader.add_value("landlord_phone", "01922 235 350")
        item_loader.add_value("landlord_email", "info@redstones.co.uk")
        item_loader.add_value("landlord_name", "Redstones")     

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
