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
import re

class MySpider(Spider):
    name = 'propertycentre_harcourts_com_au'
    execution_type='testing'
    country='australia'
    locale='en'    
    start_urls = ["https://mypropertycentre.com.au/?action=epl_search&post_type=rental&property_status=current"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id]/a[@class='property-grid-link']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )
  
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type = "".join(response.xpath("//h3/text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            p_type = "".join(response.xpath("//div[@class='epl-section-description']//text()").getall())
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
                item_loader.add_value("property_type", p_type)
            else:
                return
        item_loader.add_value("external_source", "Propertycentre_Harcourts_Com_PySpider_australia")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("zipcode"," ".join(title.split(" - ")[-2].strip().split(" ")[-2:]))
            address = title.split(" - ")[0]
            item_loader.add_value("address", address)

        city = response.xpath("//h1/div[contains(@class,'suburb')]//text()").get()
        if city:
            item_loader.add_value("city", city)

        rent = response.xpath("//span[contains(@class,'price')]//text()").get()
        if rent:
            rent = rent.split("$")[1].split(" ")[0]
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "AUD")
        script_map = response.xpath("//div[@class='single-property-map']/div/@data-cord").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split(',')[0].strip())
            item_loader.add_value("longitude", script_map.split(',')[0].strip())
        room_count = response.xpath("//li[contains(.,'Bed')]//span//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//li[contains(.,'Bath')]//span//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        parking = response.xpath("//li[contains(.,'Car')]//span//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        desc = " ".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        if "floor " in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            not_list = ["timber","made"]
            status = True
            for i in not_list:
                if i in floor.lower():
                    status = False
            if status:
                item_loader.add_value("floor", floor.replace("-","").upper())

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[contains(@class,'available')]//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.split("from")[1].split(" at")[0]
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[contains(@class,'gallery-slider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        data_cord = response.xpath("//div[contains(@class,'section-map')]//@data-cord").get()
        if data_cord:
            latitude = data_cord.split(",")[0].strip()
            longitude = data_cord.split(",")[1].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        landlord_name = response.xpath("//div[contains(@class,'agent')]//h4//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Harcourts Property Centre")

        item_loader.add_value("landlord_email", "propertycentre@harcourts.com.au")
        
        landlord_phone = response.xpath("//i[contains(@class,'mobile')]/parent::p//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "07 3397 4280")
    
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "property" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None