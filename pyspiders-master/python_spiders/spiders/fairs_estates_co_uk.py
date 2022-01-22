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
    name = 'fairs_estates_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://www.fairs-estates.co.uk/properties/?page=1&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&orderBy=Price&orderDirection=DESC&lat=&lng=&zoom=&searchbymap=&maplocations="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='photo']/a"):
            status = item.xpath("./div[@class='status']/img/@alt").get()
            if status and ("agreed" in status.lower() or status.strip().lower() == "let"):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"http://www.fairs-estates.co.uk/properties/?page={page}&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&orderBy=Price&orderDirection=DESC&lat=&lng=&zoom=&searchbymap=&maplocations="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//div[@class='description']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Fairs_Estates_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//div[@class='headline']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//div[@class='headline']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='description']//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//div[@class='beds']/text()", input_type="F_XPATH", get_num=True)
        term = response.xpath("//div[@class='price']/span[@class='displaypricequalifier']/text()").get()
        if term:
            if 'pppw' in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/span[@class='displayprice']/text()", input_type="F_XPATH", get_num=True, per_week=True)
            elif 'pcm' in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//div[@class='price']/span[@class='displayprice']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//div[@class='features']//li[contains(.,'Available')]/text()", input_type="F_XPATH", replace_list={"Available":"", "from":""})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='photocontainer']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="energy_label", input_value="//div[@class='features']//li[contains(.,'EPC Rating')]/text()", input_type="F_XPATH", split_list={"Rating":1})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//img[contains(@onload,'loadGoogleMap')]/@onload", input_type="F_XPATH", split_list={"(":1, ",":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//img[contains(@onload,'loadGoogleMap')]/@onload", input_type="F_XPATH", split_list={"(":1, ",":1})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//div[@class='features']//li[contains(.,'Parking') or contains(.,'Garage')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='features']//li[contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//div[@class='features']//li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Fairs Estates", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01912747271", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@fairs-estates.co.uk", input_type="VALUE")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "share" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None