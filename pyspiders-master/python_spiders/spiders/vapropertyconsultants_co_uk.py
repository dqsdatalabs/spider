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
    name = 'vapropertyconsultants_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.vapropertyconsultants.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=2&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.vapropertyconsultants.co.uk/properties.aspx?Mode=1&PropertyTypeGroup=1&PriceMin=0&PriceMax=0&Bedrooms=0&ShowSearch=1",
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[contains(.,'Details')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//span[contains(@id,'PropertyType')]//text()").get()
        if get_p_type_string(property_type): 
            property_type = get_p_type_string(property_type)
            item_loader.add_value("property_type", property_type)
        else: return
        item_loader.add_value("external_source", "Vapropertyconsultants_Co_PySpider_united_kingdom")

        external_id = response.xpath("//span[contains(@id,'PropertyID')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h2//text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        from datetime import datetime
        import dateparser
        if "available" in title.lower():
            available_date = title.split("Available")[1].split("-")[0].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        address = response.xpath("//h1//text()").get()
        if address:
            city = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        square_meters = response.xpath("//i[contains(@class,'area')]//parent::li//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//h2//text()").get()
        if rent:
            if "let" in rent.lower() and not "to let" in rent.lower():
                return
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//span[contains(@id,'Description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::li//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            if property_type == "studio":
                item_loader.add_value("room_count", "1")


        bathroom_count = response.xpath("//i[contains(@class,'bath')]//parent::li//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)

        item_loader.add_xpath("latitude", "substring-before(substring-after(//iframe/@src,'cbll='),',')")
        item_loader.add_xpath("longitude","substring-before(substring-after(//iframe/@src,','),'&')")

        images = [x for x in response.xpath("//div[contains(@id,'property-detail-large')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Garage') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Furnished') or contains(.,'FURNISHED')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            if "Furnished" in title:
                item_loader.add_value("furnished", True)
            
        dishwasher = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
            
        washimg_machine = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Washing Machine')]//text()").get()
        if washimg_machine:
            item_loader.add_value("washing_machine", True)
        
        pets_allowed = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Pets Allowed')]//text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        item_loader.add_value("landlord_name", "VA PROPERTY")
        item_loader.add_value("landlord_phone", "01582 346 203")
        item_loader.add_value("landlord_email", "office@vapropertyconsultants.co.uk")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None