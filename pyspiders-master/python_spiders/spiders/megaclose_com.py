# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'megaclose_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.megaclose.com/studio-apartments&filter=1"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='image']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.megaclose.com/studio-apartments?filter=1&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Megaclose_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "student_apartment")

        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h1/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//p[@class='location']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//p[@class='location']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//td[contains(.,'Price')]/following-sibling::td/text()",get_num=True, per_week=True, input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        
        desc = " ".join(response.xpath("//div[@id='tab-description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        room_count = response.xpath("//td[contains(.,'Bedroom')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "studio" in desc.lower():
            item_loader.add_value("room_count", "1")
        
        if "sq" in desc:
            square_meters = desc.split("sq")[0].replace("\u00a0"," ").replace("is","").strip().split(" ")[-1]
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        import dateparser
        if "Tenancy length -" in desc:
            available_date = desc.split("Tenancy length -")[1].split("to")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//td[contains(.,'Bathroom')]/following-sibling::td/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[contains(@class,'gallery-thumbs')]//@src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="floor_plan_images", input_value="//a[contains(@href,'floorplan')]/@href", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={'Lng(':1, ',':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'Lng')]/text()", input_type="M_XPATH", split_list={'Lng(':1, ',':1, ')':0})
        ItemClear(response=response, item_loader=item_loader, item_name="elevator", input_value="//td[contains(.,'Lift')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="washing_machine", input_value="//td[contains(.,'Washing') or contains(.,'Washer')]//text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="MEGACLOSE", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0115 911 2200", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="lettings@megaclose.com", input_type="VALUE")
        
        yield item_loader.load_item()