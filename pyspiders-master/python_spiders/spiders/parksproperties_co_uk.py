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
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'parksproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['http://www.parksproperties.co.uk/lettings/']  # LEVEL 1
    
    custom_setting={
        "HTTPCACHE_ENABLED": False
    }

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//ul[@class='row list-unstyled']/li"):
            follow_url = response.urljoin(item.xpath("./div/a/@href").get())
            # property_type = item.xpath(".//span[@class='type']/text()").get()
            # if get_p_type_string(property_type):
            #     property_type = get_p_type_string(property_type)
            #     yield Request(follow_url, callback=self.populate_item, meta={"property_type": property_type})
            # else:
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            header = {
                "Proxy-Connection": "keep-alive",
                "Referer": "http://www.parksproperties.co.uk/",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
            }
            url = f"http://www.parksproperties.co.uk/lettings/page/{page}/"
            yield Request(url, callback=self.parse, headers=header, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        description = "".join(response.xpath("//div[@class='content-box']//p//text()").getall())
        item_loader.add_value("external_link", response.url)
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: return

        item_loader.add_value("external_source", "Parksproperties_Co_PySpider_united_kingdom")

        external_id = response.xpath("substring-after(//link[@rel='shortlink']/@href,'=')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//h1//text()").getall())
        if address:
            city = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        rent = "".join(response.xpath("//div[contains(@class,'property-header')]//div[contains(@class,'meta')]//text()").getall())
        if rent:
            rent = rent.split("£")[1].split("·")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'main-content')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'property-meta primary-tooltips')]//i[contains(@class,'bed')]//parent::div//following-sibling::div//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'property-meta primary-tooltips')]//i[contains(@class,'tint')]//parent::div//following-sibling::div//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'property-image-container')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        available_date = "".join(response.xpath("//i[contains(@class,'calendar')]//parent::div//following-sibling::div//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//li[contains(.,'Washmachine')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "PARKS PROPERTIES")
        item_loader.add_value("landlord_phone", "02076192666")
        item_loader.add_value("landlord_email", "info@parksproperties.co.uk")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower() or "residential" in p_type_string.lower() or "conversion" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None