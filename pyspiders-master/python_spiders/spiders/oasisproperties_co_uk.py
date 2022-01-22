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
    name = 'oasisproperties_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Oasisproperties_Co_PySpider_united_kingdom'
    start_urls = ['https://www.oasisproperties.co.uk/student-accommodation-search/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):        
        for url in response.xpath("//div[@id='listing_ajax_container']//h4/a/@href").getall():
            yield Request(url, callback=self.populate_item)     

        next_page = response.xpath("//ul[@class='pagination pagination_nojax']/li[@class='roundright']/a/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse)     


    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        description = "".join(response.xpath("//div[@class='property_title_label actioncat']//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: 
            description = "".join(response.xpath("//div[@id='description']//text()[normalize-space()] | //h1/text()").getall())
            if get_p_type_string(description):
                item_loader.add_value("property_type", get_p_type_string(description))
            else: 
                return
        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//strong[contains(.,'Property Id')]//parent::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//div[contains(@class,'property_categs')]//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city = response.xpath("//strong[contains(.,'City')]//parent::div//a//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("//h1[@class='entry-title entry-prop']/text()").get()
        if zipcode and "," in zipcode:
            item_loader.add_value("zipcode", zipcode.split(',')[-1].split('Leeds ')[-1].strip())

        rent = "".join(response.xpath("//strong[contains(.,'Price')]//parent::div/text()").getall())
        if rent:
            rent = rent.split("£")[1].strip()
            rent = int(float(rent))*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//strong[contains(.,'Deposit')]//parent::div/text()").get()
        if deposit:
            deposit = deposit.strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@id,'description')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//strong[contains(.,'Bedroom')]//parent::div/text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//strong[contains(.,'Bathroom')]//parent::div/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        latitude = response.xpath("//div[@id='googleMap_shortcode']/@data-cur_lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='googleMap_shortcode']/@data-cur_long").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        images = [x for x in response.xpath("//li[contains(@data-target,'carousel-listing')]/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        else:
            images = [x.split('url(')[1].split(')')[0] for x in response.xpath("//div[contains(@class,'image_gallery')]/@style").getall()]
            if images:
                item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@class,'floor_image')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//strong[contains(.,'Available')]//parent::div/text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        dishwasher = response.xpath("//div[contains(@class,'listing_detail')]//text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_name", "Oasis Properties")
        item_loader.add_value("landlord_phone", "0113 230 6522")
        item_loader.add_value("landlord_email", "info@oasisproperties.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None