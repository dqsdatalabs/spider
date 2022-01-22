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

class MySpider(Spider):
    name = 'gnre_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source= 'Gnre_Com_PySpider_australia'

    start_urls = ["http://gnre.com.au/result.php?ptr=r&sub=all-sub&rpwl=0&rpwh=99999&sort=date-desc&con=L"]

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1)

        for item in response.xpath("//div[@class='column-third']//a//@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        if response.xpath("//a[contains(.,'Next')]/@href").get():
            new_url = f"http://gnre.com.au/result.php?ptr=r&sub=all-sub&rpwl=0&rpwh=99999&sort=date-desc&con=L&pg={page}"
            yield Request(new_url, callback=self.parse, meta={"page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        p_type = "".join(response.xpath("//strong[contains(.,'Property Type')]/..//text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            return
        
        from python_spiders.helper import ItemClear
        
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//div[contains(@class,'p7TP3_content_16')]//h2//text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p/strong[contains(.,'Address')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p/strong[contains(.,'Address')]/following-sibling::text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//p/strong[contains(.,'Rent')]/following-sibling::text()", input_type="F_XPATH", get_num=True, per_week=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="USD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='property-meta']//span[2]//text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//div[@class='property-meta']//span[1]//text()", input_type="F_XPATH", get_num=True)
        
        desc = " ".join(response.xpath("//div[contains(@class,'p7TP3_content_16')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//p/strong[contains(.,'Id')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p/strong[contains(.,'Availability')]/following-sibling::text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//p/strong[contains(.,'Bond')]/following-sibling::text()", input_type="F_XPATH", get_num=True, split_list={"$":1}, replace_list={",":"."})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//p/strong[contains(.,'Total Parking') or contains(.,'Garage')]/following-sibling::text()[.!='0']", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'p7IGM03_thumbslist')]//a//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//h4[contains(.,'Agent Info')]/following-sibling::p/text()[1]", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//h4[contains(.,'Agent Info')]/following-sibling::p/text()[2]", input_type="F_XPATH", split_list={":":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//h4[contains(.,'Agent Info')]/following-sibling::p/a/text()", input_type="F_XPATH")
        
            
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None