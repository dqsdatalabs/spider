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
    name = 'spaceleeds_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.spaceleeds.co.uk/property-search?field_bedrooms_tid=All&field_area_tid=All&field_price_value_1=All&field_price_value=All&field_bathrooms_value=All']  # LEVEL 1

    custom_settings = {"PROXY_ON": "True"}
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//div[@class='field-content']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            url = f"https://www.spaceleeds.co.uk/property-search?field_bedrooms_tid=All&field_area_tid=All&field_price_value_1=All&field_price_value=All&page={page}&field_bathrooms_value=All"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[contains(@class,'field-item')]//*[self::p or self::ul]//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Spaceleeds_Co_PySpider_united_kingdom")

        address = "".join(response.xpath("//div[@class='adr']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        else:
            item_loader.add_xpath("address", "//div[contains(@class,'field-item even')]/h2/text()")
            
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//span[@class='postal-code']/text()", input_type="F_XPATH")
        if response.xpath("//script[contains(.,'latlons')]//text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='locality']/text()", input_type="F_XPATH")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="Leeds", input_type="VALUE")

        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'field-item even')]/h2/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[contains(@class,'summary')]//p//text()", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[contains(@class,'bedroom')]//text()", input_type="F_XPATH", get_num=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='field-label' and contains(.,'Bathroom')]/following-sibling::div//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[contains(@class,'price')]//text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='field-label' and contains(.,'Availabil')]/following-sibling::div//text()", input_type="F_XPATH",replace_list={"20221":"2021"})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//div[contains(@class,'deposit')]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div[contains(@class,'field-reference')]//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'field-type-image')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'parking') or contains(.,'Parking')]//text()", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//li[contains(.,'balcon') or contains(.,'Balcon')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,' furnished') or contains(.,'Furnished') or contains(.,'furniture ')]//text()", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//li[contains(.,'lift') or contains(.,'Lift')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li[contains(.,'terrace') or contains(.,'Terrace')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Space Lettings", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0113 2744 295", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="spaceleeds01@gmail.com", input_type="VALUE")
        latitude_longitude = response.xpath("//script[contains(.,'latlons')]//text()").get()
        if latitude_longitude:  
            item_loader.add_value("latitude", latitude_longitude.split('"latlons":[["')[-1].split('"')[0])
            item_loader.add_value("longitude", latitude_longitude.split('"latlons":[["')[-1].split(',"')[1].split('"')[0])
      
        
        status = response.xpath("//div[@class='field-label' and contains(.,'let')]/following-sibling::div//text()").get()
        if "Available" in status:
            yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None