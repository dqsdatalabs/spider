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

class MySpider(Spider):
    name = 'charlessinclair_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    start_urls = ['https://www.charlessinclair.com/?id=43248&action=view&route=search&view=list&input=SW4&jengo_property_for=2&jengo_category=1&jengo_radius=-1&jengo_property_type=-1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_bathrooms=0&jengo_max_bathrooms=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=on&pfor_offer=on&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property-feed-list']//div[@class='imgLink relative']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
         
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//span[@class='property_detail__status']/text()").get()
        if 'let agreed' in status.lower():
            return
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2])
        if get_p_type_string(response.url):
            property_type = get_p_type_string(response.url)
        else: return
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_source", "Charlessinclair_PySpider_united_kingdom")
        
        item_loader.add_xpath("title", "//title/text()")

        lat = response.xpath("//script[contains(.,'latitude')]/text()").re_first(r'"latitude":"(\d+.\d+)",')
        lng = response.xpath("//script[contains(.,'latitude')]/text()").re_first(r'"longitude":"(-*\d+.\d+)",')
        if lat and lng:
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
    
        rent = response.xpath("//div[@class='Letdetails detail___price']/text()").get()
        if rent:                                 
            item_loader.add_value("rent_string", rent.replace("PCM", "").strip()) 
        
        description = "".join(response.xpath("//div[@id='description']//p/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        address = response.xpath("//h4/text()").get()
        if address:  
            item_loader.add_value("address", address)
            city = address.split(",")[-2].strip()
            if city:
                item_loader.add_value("city",city)
            zipcode = address.split(",")[-1].strip()
            if zipcode:
                item_loader.add_value("zipcode", zipcode)

        room_count = response.xpath("//li[contains(.,'Bedroom')]//text()").re_first(r'\d')
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'Bath')]//text()").re_first(r'\d')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
    
        features = ", ".join(response.xpath("//div[@id='features']//text()").extract())
        if features:
            if "parking" in features.lower():
                item_loader.add_value('parking', True)
            
            if "terrace" in features.lower():
                item_loader.add_value('terrace', True)

            if "swimming pool" in features.lower():
                item_loader.add_value('swimming_pool', True)

            if "elevator" in features.lower() or "lift" in features.lower():
                item_loader.add_value('elevator', True)
            
            if "balcony" in features.lower():
                item_loader.add_value('balcony', True)

            if "dishwasher" in features.lower():
                item_loader.add_value('dishwasher', True)
            if "washing machine" in features.lower():
                item_loader.add_value('washing_machine', True)
            if 'furnished' in features.lower():
                item_loader.add_value("furnished", True)

        available_date = response.xpath("//span[@class='badge pd__badge pd__availability']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date)
            if date_parsed:                
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        images = [x for x in response.xpath("//div[@id='galleria']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        item_loader.add_value("landlord_name", "Charles Sinclair")
        item_loader.add_value('landlord_phone', '020 7622 1180')
        item_loader.add_value("landlord_email", "lettings@charlessinclair.com")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None