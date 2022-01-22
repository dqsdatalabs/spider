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
    name = 'cliftonpropertypartners_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://cliftonpropertypartners.com/properties/rentals"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='prop_item_inner']"):
            status = "".join(item.xpath("./div[@class='prop_image']/div/text()").getall())
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./div[contains(@class,'prop_submit_wrap')]/a/@href").get())
            yield Request(follow_url, callback=self.populate_item)        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='esgrail-center']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cliftonpropertypartners_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"/property":0, "/":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="London", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='esgrail-center']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//text()[contains(.,'sq ft') or contains(.,'Sq Ft') or contains(.,'sq.') or contains(.,'square feet') or contains(.,'Square Feet')]", input_type="F_XPATH",split_list={"sq":0,"Sq Ft":0}, get_num=True, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='propertydata' and contains(text(),'Bedroom')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='propertydata' and contains(text(),'Bathroom')]/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='lightgallerywrap']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[contains(.,'Download Floorplan')]/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='esgrail-center']//p//text()[contains(.,'Parking') or contains(.,'Garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//div[contains(text(),'Balcony')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//div[contains(text(),'Lift')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//div[contains(text(),'Terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//div[contains(text(),'Swimming Pool') or contains(text(),'Swimming pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Clifton Property Partners", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="+44 (0)20 7409 5087", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@cliftonpp.com", input_type="VALUE")

        rent = response.xpath("//div[contains(text(),'Property Price')]/strong/text()").get()
        if rent:
            if "-" in rent:
                item_loader.add_value("room_count", "1")
                rent = rent.split("£")[1].split("-")[0]
                item_loader.add_value("rent", int(rent)*4)
            else:
                rent = rent.split("£")[1].replace(",","")
                item_loader.add_value("rent", int(rent)*4)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None