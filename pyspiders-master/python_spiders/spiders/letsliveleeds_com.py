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
    name = 'letsliveleeds_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.letsliveleeds.com/search_gallery.php?town=&bedrooms=houseshare']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='propertyContent']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@class='navbynumbers_Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//li[contains(.,'Property Type')]/span/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return
        item_loader.add_value("external_source", "Letsliveleeds_PySpider_united_kingdom")

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//li[contains(.,'Location')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@style,'margin-bottom')][2]/p//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li[contains(.,'Bedroom')]/span/text()[.!='0']", input_type="F_XPATH", get_num=True)
        
        term = response.xpath("//p[@class='price-alt']/text()").get()
        if "pw" in term:
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='price-alt']/text()", input_type="F_XPATH", get_num=True, per_week=True, replace_list={"pw":"", ",":""})
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p[@class='price-alt']/text()", input_type="F_XPATH", get_num=True, split_list={"p":0}, replace_list={",":""})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//li[contains(.,'Available')]/span/text()[not(contains(.,'Now'))]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"id=":-1})
        if response.xpath("//ul[@class='bxslider2']//@src").get():
            ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[@class='bxslider2']//@src", input_type="M_XPATH")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='gallery']/img/@src[not(contains(.,'missing.png'))]", input_type="M_XPATH")

        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'LatLng(')]/text()", input_type="F_XPATH", split_list={"LatLng(":1,",":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]/span/text()[contains(.,'Yes')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Lets Live Leeds", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0113 887 8605", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="admin@letsliveleeds.com", input_type="VALUE")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None