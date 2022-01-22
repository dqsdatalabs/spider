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
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'apgproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ["http://www.apgproperty.com.au/property_type/renting/"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@class,'property-post')]/div/div/a"):
            status = item.xpath("./img/@alt").get()
            if status and "leased" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@class,'next ')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//h3[@class='dtl_title']/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@class='pf-content']//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Apgproperty_Com_PySpider_australia", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='pf-content']/h4/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//div[@class='pf-content']/h4/text()", input_type="F_XPATH", split_list={" ":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h3[@class='dtl_title']/text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='pf-content']//p//text()", input_type="M_XPATH")
        
        if response.xpath("//span[@class='dw_bedroom']/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[@class='dw_bedroom']/text()", input_type="F_XPATH", get_num=True)
        elif response.xpath("//h3[@class='dtl_title']/text()[contains(.,'Bedroom')]"):
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//h3[@class='dtl_title']/text()", input_type="M_XPATH", split_list={"Bed":0, " ":-1})
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[@class='dw_bathroom']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='pp_price']/text()", input_type="F_XPATH", get_num=True, per_week=True, split_list={" ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="AUD", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//ul[contains(@class,'image-gallery')]//@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//span[@class='dw_garage']/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="balcony", input_value="//label[contains(.,'Balcon')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//label[contains(.,'Pool')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="dishwasher", input_value="//label[contains(.,'Dishwasher')]", input_type="F_XPATH", tf_item=True)
        
        if response.xpath("//div[contains(@class,'ageninfo_right')]/h3/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="//div[contains(@class,'ageninfo_right')]/h3/text()", input_type="F_XPATH")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="APG ESTATE", input_type="VALUE")
            
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="//span[@class='detail']/text()[contains(.,'Phone')] |//div[contains(@class,'dtl_field l')]/p[contains(.,'Mobile')]/text()", input_type="F_XPATH", split_list={":":1})
        
        if response.xpath("//span[@class='detail'][contains(.,'Email')]/span/text()"):
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="//span[@class='detail'][contains(.,'Email')]/span/text()", input_type="F_XPATH")
        else:
            ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@apgproperty.com.au", input_type="VALUE")
            
        city = response.xpath("//div[@class='pf-content']/h4/text()").get()
        if city:
            if "," in city:
                city = city.split(",")[-2].strip()
                if not city.split(" ")[0].isalpha():
                    not_alpha = city.split(" ")[0]
                    item_loader.add_value("city", city.split(not_alpha)[1].strip())
                else:
                    item_loader.add_value("city",city)
            else:
                city = city.split("VIC")[0].strip()
                if not city.split(" ")[0].isalpha():
                    not_alpha = city.split(" ")[0]
                    if not_alpha:
                        item_loader.add_value("city", city.split(not_alpha)[1].strip())
                    else: item_loader.add_value("city",city)
                else:
                    item_loader.add_value("city",city)
                    
        status = response.xpath("//img/@alt[contains(.,'Lease ') or contains(.,'lease-banner')]").get()
        if not status:
            leased_banner = response.xpath("//div[contains(@class,'dtl_thubimg')]/div/img/@alt").get()
            if "無標題-1" in leased_banner or "1444446728675" in leased_banner:
                return
            yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "unit" in p_type_string.lower() or "home" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    else:
        return None