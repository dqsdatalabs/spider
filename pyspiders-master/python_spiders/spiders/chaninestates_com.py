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
    name = 'chaninestates_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.chaninestates.com/notices.php?c=44|46"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 1)
        seen = False
        for item in response.xpath("//div[@class='list-holder']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            p_url = f"https://www.chaninestates.com/template/estate_template_13/load_notices_estate_grid_chanin.php?p={page}&q=&l=&c=44|46&price_sort=&g=&&&"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[contains(text(),'Type')]/text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//div[@class='detail_content']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Chaninestates_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h3[contains(@class,'title')]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//li/div[contains(.,'Bedroom')]//text()", input_type="M_XPATH", split_list={" ":0 })
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//div[@class='detail_content']//text()[contains(.,'Floor area')]", input_type="M_XPATH", split_list={":":1, ".":0}, sq_ft=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//li/div[contains(.,'Bathroom')]//text()", input_type="M_XPATH", split_list={" ":0 })
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//img[@class='rsImg']/@src", input_type="M_XPATH")
        # ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//img[contains(@alt,'Floor Plan')]/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div/b[contains(.,'Available')]/parent::div//text()", input_type="M_XPATH", split_list={":":1 })
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//div/b[contains(.,'Reference')]/parent::div//text()", input_type="M_XPATH", split_list={":":1 })
        ItemClear(response=response, item_loader=item_loader, item_name="terrace", input_value="//li/div[contains(.,'Terrace')]//text()", input_type="M_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'showMap')]/text()", input_type="F_XPATH", split_list={'showMap(':2, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'showMap')]/text()", input_type="F_XPATH", split_list={'showMap(':2, ',':1, ")":0})
        
        
        desc = " ".join(response.xpath("//div[@class='detail_content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
            
            if "EPC RATING" in desc:
                energy_label = desc.split("EPC RATING")[1].strip().split(" ")[0]
                item_loader.add_value("energy_label", energy_label)
            
        address = "".join(response.xpath("//div[@class='map_location']//text()").getall())
        if address:
            address = address.strip()
            item_loader.add_value("address", address)
            zipcode = address.split(" ")[-1]
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            if "pw" in rent:
                rent = rent.split("pw")[0].split("£")[1].strip().replace(",","")
                item_loader.add_value("rent", int(rent)*4)
        
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="CHANIN ESTATES", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="44 2071000888", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@chaninestates.com", input_type="VALUE")

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