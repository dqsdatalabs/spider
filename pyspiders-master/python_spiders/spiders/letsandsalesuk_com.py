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
    name = 'letsandsalesuk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.' 
  
    def start_requests(self):
        start_url = "https://www.letsandsalesuk.com/wp-admin/admin-ajax.php?search_location=&lat=&lng=&use_radius=on&search_radius=3&location%5B%5D=&status%5B%5D=let&bedrooms=&bathrooms=&min-area=&max-area=&property_id=&min-price=200&max-price=2500000&action=houzez_half_map_listings&paged=1&sortby=&item_layout=v1"
        yield Request(start_url, callback=self.parse)
        

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen = False
        data = json.loads(response.body)
        if int(data["total_results"])>0:
            for item in data["properties"]:
                url = item["url"]
                lat = item["lat"]
                lng = item["lng"]
                yield Request(url, callback=self.populate_item,meta={"lat":lat,"lng":lng})
                seen = True
        if page == 2 or seen:
            page_url = f"https://www.letsandsalesuk.com/wp-admin/admin-ajax.php?search_location=&lat=&lng=&use_radius=on&search_radius=3&location%5B%5D=&status%5B%5D=let&bedrooms=&bathrooms=&min-area=&max-area=&property_id=&min-price=200&max-price=2500000&action=houzez_half_map_listings&paged={page}&sortby=&item_layout=v1"
            yield Request(page_url, callback=self.parse, meta={"page": page+1})

       
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Letsandsalesuk_PySpider_united_kingdom")

        prp_type = "".join(response.xpath("//ul[li[.='Property Type']]/li[@class='property-overview-item']//text()").getall())
        if get_p_type_string(prp_type):
            item_loader.add_value("property_type", get_p_type_string(prp_type))
        else: return
        lat = response.meta.get('lat')
        lng = response.meta.get('lng')
        item_loader.add_value("latitude", lat)
        item_loader.add_value("longitude", lng)
      
        external_id = response.xpath("//div[strong[.='Property ID:']]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        item_loader.add_xpath("title", "//div[@class='page-title-wrap']//h1/text()")
      
        item_loader.add_xpath("city", "//li[strong[.='City']]/span/text()")
        item_loader.add_xpath("address", "//li[strong[.='Address']]/span/text()")
        item_loader.add_xpath("zipcode", "//li[strong[.='Zip/Postal Code']]/span/text()")
       
        item_loader.add_xpath("rent_string", "//div[@class='page-title-wrap']//li[@class='item-price']//text()")
        desc = " ".join(response.xpath("//div[@class='block-content-wrap']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_xpath("room_count", "//ul[li[.='Bedrooms' or .='Bedroom']]//li[@class='property-overview-item']/strong/text()")
        item_loader.add_xpath("bathroom_count", "//ul[li[.='Bathrooms' or .='Bathroom']]//li[@class='property-overview-item']/strong/text()")
        
        parking = response.xpath("//li/a[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
                
        images = [x for x in response.xpath("//div[@id='property-gallery-js']//img[@class='img-fluid']/@src").getall()]
        if images:
            item_loader.add_value("images", images)         
     
        item_loader.add_value("landlord_name", "The Lettings & Sales")
        item_loader.add_value("landlord_phone", "0300 124 5656")
        item_loader.add_value("landlord_email", "info@letsandsalesuk.com")
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