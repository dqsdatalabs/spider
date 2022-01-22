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
    name = 'sapphirehomes_co_uk_disabled'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.sapphirehomes.co.uk/search/?showstc=off&showsold=on&address_keyword_exact=1&instruction_type=Letting&address_keyword=&minprice=&maxprice="]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='thumbnail']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            room_count = item.xpath("./../..//div[contains(@class,'property-rooms')]//img[contains(@alt,'bedroom')]/following-sibling::text()").get()
            bathroom_count = item.xpath("./../..//div[contains(@class,'property-rooms')]//img[contains(@alt,'bathroom')]/following-sibling::text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"room_count":room_count, "bathroom_count":bathroom_count})
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.sapphirehomes.co.uk/search/{page}.html?showstc=off&showsold=on&address_keyword_exact=1&instruction_type=Letting&address_keyword=&minprice=&maxprice="
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url.split("?")[0])
        f_text = " ".join(response.xpath("//div[@class='propDesc']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value=response.url, input_type="VALUE", split_list={"property-details/":-1, "/":0})
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Sapphirehomes_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//h1[@class='resultHead']/span[@itemprop='name']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//h1[@class='resultHead']/span[@itemprop='name']/text()", input_type="F_XPATH", split_list={",":-2})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//h1[@class='resultHead']/span[@itemprop='name']/text()", input_type="F_XPATH", split_list={",":-1})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//div[@class='propDesc']//h1/following-sibling::text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=response.meta["room_count"], input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value=response.meta["bathroom_count"], input_type="F_XPATH")
        term = response.xpath("//span[@itemprop='price']/text()").get() if response.xpath("//span[@itemprop='price']/text()").get() else ""
        if "Per Week" in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@itemprop='price']/text()", input_type="F_XPATH", get_num=True, per_week=True)
        elif "PCM" in term: ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@itemprop='price']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@id='property-thumbnails']//div[@class='carousel-inner']//img/@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'ShowMap')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'ShowMap')]/text()", input_type="F_XPATH", split_list={"&q=":1, "%2C":1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="parking", input_value="//li[contains(.,'Parking')]", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Unfurnished')]", input_type="F_XPATH", tf_item=True, tf_value=False)
        ItemClear(response=response, item_loader=item_loader, item_name="furnished", input_value="//li[contains(.,'Furnished')]", input_type="F_XPATH", tf_item=True, tf_value=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Sapphire Homes", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="01942 494944", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@sapphirehomes.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    else:
        return None