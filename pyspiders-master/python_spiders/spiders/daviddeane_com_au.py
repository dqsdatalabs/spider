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
    name = 'daviddeane_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    start_urls = ['https://daviddeane.com.au/listings?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&paged=2']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        data = response.xpath("//script[contains(.,'MapDataStore')]/text()").get()
        data = data.split("= ")[1].split(";")[0].strip()
        d_json = json.loads(data)
        
        for item in d_json:
            yield Request(item["url"], callback=self.populate_item, meta={"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        
        description = "".join(response.xpath("//div[@class='b-description__text']//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return
        
        item_loader.add_value("external_source", "Daviddeane_Com_PySpider_australia")
        item = response.meta.get('item')

        item_loader.add_value("address", item["address"])
        item_loader.add_value("city", item["address"].split(",")[-1].strip())
        
        title = response.xpath("//h5[contains(@class,'post-title')]/text()").get()
        item_loader.add_value("title", title)
        
        room_count = response.xpath("//p[contains(@class,'bed')]/text()").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//p[contains(@class,'bath')]/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        rent = response.xpath("//p[contains(@class,'price')]/text()").get()
        if rent:
            price = rent.split(" ")[0].split("$")[1].strip()
            item_loader.add_value("rent", int(price)*4)
        item_loader.add_value("currency", "AUD")
        
        description = " ".join(response.xpath("//div[contains(@class,'post-content')]//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        deposit = response.xpath("//div/strong[contains(.,'bond')]/following-sibling::text()").get()
        if deposit:
            deposit = deposit.split("$")[1].strip()
            item_loader.add_value("deposit", deposit)
        
        parking = response.xpath("//p[contains(@class,'car')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        swimming_pool = response.xpath("//li[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        images = [x.split("'")[1] for x in response.xpath("//div[contains(@class,'slides')]//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = response.xpath("//div/strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        import dateparser
        available_date = response.xpath("//p/text()[contains(.,'Available') or contains(.,'AVAILABLE')]").get()
        if available_date:
            available_date = available_date.lower().split("available")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("latitude", item["Lat"])
        item_loader.add_value("longitude", item["Long"])
        
        item_loader.add_value("landlord_name", "DAVID DEANE")
        item_loader.add_value("landlord_phone", "07 3817 6666")
        item_loader.add_value("landlord_email", "property@daviddeane.com.au")
        
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