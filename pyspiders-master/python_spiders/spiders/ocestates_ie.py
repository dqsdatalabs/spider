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
    name = 'ocestates_ie'
    execution_type='testing'
    country='ireland'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'       

    def start_requests(self):
        start_url = "https://www.ocestates.ie/properties/lettings/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listings']//article"):
            follow_url = response.urljoin(item.xpath("./div//a/@href").get())
            is_available = item.xpath(".//h6[@class='available']").get()
            property_type = item.xpath(".//ul[@class='property-details-list']/li[1]/text()").get()
            if is_available and property_type:
                if get_p_type_string(property_type):
                    yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
    
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Ocestates_PySpider_ireland", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//link[@rel='shortlink']/@href", input_type="F_XPATH", split_list={"?p=":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//ul[@class='property-details-list']/li[contains(.,'terrace')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//ul[@class='property-details-list']/li/img[contains(@src,'bed')]/parent::li/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//ul[@class='property-details-list']/li/img[contains(@src,'bath')]/parent::li/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//section[@id='section_overview']//p/text()[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        # ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"lat =":1, ";":0})
        # ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lat')]/text()", input_type="F_XPATH", split_list={"lng =":1, ";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property_gallery']//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//section[@id='section_overview']//p/text()[contains(.,'Washing')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//section[@id='section_overview']//p/text()[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//section[@id='section_overview']//p/text()[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//section[@id='section_overview']//p/text()[contains(.,'Balcony')]", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//section[@id='section_overview']//p/text()[contains(.,'Available')]", input_type="F_XPATH", split_list={"Available":1})
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="O'Connor Estate Agents", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="353-1-860-3997", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@ocestates.ie", input_type="VALUE")

        zipcode = response.xpath("//h1/text()").get()
        if zipcode:
            zipcode = zipcode.split(" ")[-1]
            if not zipcode.isalpha():
                item_loader.add_value("zipcode", zipcode)
        
        energy_label = response.xpath("//div[@class='property-ber']//@src").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("-")[-1].split(".")[0].upper())
        
        desc = " ".join(response.xpath("//section[@id='section_details']/p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        rent = " ".join(response.xpath("//div[@class='property-price']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
            
        yield item_loader.load_item()
    
def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None