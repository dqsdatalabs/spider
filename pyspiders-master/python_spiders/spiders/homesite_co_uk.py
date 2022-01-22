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
    name = 'homesite_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://homesite.co.uk/property-search/?department=residential-lettings&officeID=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=&sale_by=&is_auction=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False

        for item in response.xpath("//ul[contains(@class,'properties')]/li"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'More Details')]/@href").get())
            prop_type = item.xpath(".//div[contains(@class,'info type')]/text()").get()
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})
            seen = True
         
        if page == 2 or seen:
            url = f"https://homesite.co.uk/property-search/page/{page}/?department=residential-lettings&officeID&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&sale_by&is_auction"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Homesite_PySpider_united_kingdom")
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("p=")[-1])

        address = response.xpath("//h1/span/text()").get()
        if address:
            item_loader.add_value("title", address.strip())
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city)

        rent = "".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            rent = rent.split("£")[1].replace(",","").strip().split(" ")[0]
            item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//li[span[contains(.,'Bedroom')]]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//li[span[contains(.,'Bathroom')]]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        import dateparser
        available_date = response.xpath("//li[span[contains(.,'Available')]]/text()").get()
        if available_date and "now" not in available_date.lower():
            available_date= available_date.strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        square_meters = response.xpath("//li[contains(.,'m2')]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m2")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", int(float(square_meters)))
        squarecheck=item_loader.get_output_value("square_meters")
        if not squarecheck:
            squ=response.xpath("//li[contains(text(),'sqm')]/text()").get()
            if squ:
                squ=squ.split("/")[-1].split(".")[0].split("s")[0].strip()
                item_loader.add_value("square_meters",squ)

        
        desc = " ".join(response.xpath("//div[@class='description-contents']//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        images = [x for x in response.xpath("//ul[@class='slides']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//li[contains(@class,'floorplans')]//@href").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        elevator = response.xpath("//li[contains(.,'lift') or contains(.,'Lift')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//li[contains(.,'balcon') or contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_name", "Homesite")
        item_loader.add_value("landlord_phone", "02072433535")
        item_loader.add_value("landlord_email", "info@homesite.co.uk")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None