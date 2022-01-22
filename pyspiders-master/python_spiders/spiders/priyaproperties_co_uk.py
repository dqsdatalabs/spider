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
    name = 'priyaproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://priyaproperties.co.uk/search/?department=residential-lettings&address_keyword=&radius=&marketing_flag=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&availability=&minimum_bedrooms=&maximum_bedrooms=&minimum_floor_area=&maximum_floor_area=&commercial_property_type=&lat=&lng=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//a[contains(.,'More Detail')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://priyaproperties.co.uk/search/page/{page}/?department=residential-lettings&address_keyword&radius&marketing_flag&minimum_price&maximum_price&minimum_rent&maximum_rent&availability&minimum_bedrooms&maximum_bedrooms&minimum_floor_area&maximum_floor_area&commercial_property_type&lat&lng"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//li[@class='property-type']/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        item_loader.add_value("external_source", "Priyaproperties_Co_PySpider_united_kingdom")

        status = response.xpath("//li[contains(.,'Availability')]/text()").get()
        if "let agreed" in status.lower():
            return
        externalid=response.xpath("//li[@class='reference-number']/span/following-sibling::text()").get()
        if externalid:
            item_loader.add_value("external_id",externalid)
        zipcode = response.xpath("//h1/text()").get()
        if zipcode:
            zipcode = zipcode.split(",")[-1].strip().split(" ")
            item_loader.add_value("zipcode", f"{zipcode[-2]} {zipcode[-1]}")
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1," ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description-contents']//p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedrooms:')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li[contains(.,'Bathrooms:')]/text()", input_type="F_XPATH", get_num=True)
        
        term = response.xpath("//div[@class='price']/text()").get()
        if "pw" in term.lower():
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/text()", input_type="F_XPATH", get_num=True, split_list={" ":0})

        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")        
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available:')]/text()[not(contains(.,'Now'))]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//li[contains(.,'Deposit')]/text()", input_type="F_XPATH", get_num=True, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='slides']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/text()[contains(.,'Furnished') or contains(.,' furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'Terrace')]/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Priya Properties", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0116 255 9950", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="findmeahome@priyaproperties.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None