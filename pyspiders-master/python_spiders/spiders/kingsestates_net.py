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
    name = 'kingsestates_net'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.' 
    start_urls = ['https://www.kingsestates.net/search/?instruction_type=Letting&showstc=on&address_keyword=&minprice=&maxprice=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='property']"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Full')]/@href").get())
            status = item.xpath(".//h3[contains(.,'Let Agreed')]//text()").get()
            if status:
                continue
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.kingsestates.net/search/{page}.html?instruction_type=Letting&showstc=on&address_keyword=&minprice=&maxprice="
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//div[contains(@class,'hidden')]//p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Kingsestates_PySpider_united_kingdom")
        item_loader.add_value("external_id",response.url.split("property-details/")[-1].split("/")[0])

        item_loader.add_xpath("title", "//head/title/text()")
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
            if "," in address:
                item_loader.add_value("city", address.split(",")[-1].strip())
       
        desc = " ".join(response.xpath("//div[@class='col-md-5']/div/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "//div[@class='room-icons']//span[svg[@class='icon-bedrooms']]/strong/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='room-icons']//span[svg[@class='icon-bathrooms']]/strong/text()")
        
        parking = response.xpath("//li[contains(.,'Car Park') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        balcony = response.xpath("//li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        energy_label = response.xpath("//div/strong[.='RATING:']/following-sibling::strong[1]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        furnished = response.xpath("//li[text()=' Furnished']/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        latitude_longitude = response.xpath("//script[contains(.,'latitude') and contains(.,'longitude')]/text()").get()
        if latitude_longitude:  
            item_loader.add_value("latitude", latitude_longitude.split('"latitude": "')[-1].split('"')[0])
            item_loader.add_value("longitude", latitude_longitude.split('"longitude": "')[-1].split('"')[0])
      
        images = [response.urljoin(x) for x in response.xpath("//div[@id='property-carousel']//div[@class='carousel-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)         
 
        item_loader.add_xpath("rent_string", "//h2/text()")
     
        item_loader.add_value("landlord_name", "Kings Estates")
        landlord_phone = response.xpath("//div[@class='property-contact-box text-center']/p[@class='property-tel']//text()").get()
        item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_xpath("landlord_email", f"//div[@class='row']/div[p/a[contains(.,'{landlord_phone}')]]//a[contains(@href,'mail')]/text()")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "room" in p_type_string.lower() or "bedsit" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None