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

class MySpider(Spider):
    name = 'ldg_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.ldg.co.uk/property-search/?department=residential-lettings&minimum_bedrooms&maximum_bedrooms&minimum_floor_area&maximum_floor_area']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[@class='thumbnail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath("./div[@class='flag']/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.ldg.co.uk/property-search/page/{page}/?department=residential-lettings&minimum_bedrooms&maximum_bedrooms&minimum_floor_area&maximum_floor_area"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[@class='description']//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Ldg_Co_PySpider_united_kingdom")  
        item_loader.add_xpath("title", "//h1/text()")
        furnished = response.xpath("//li[@class='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        available_date = response.xpath("substring-after(//li[@class='available']/text(),':')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %B %Y"], languages=['fr'])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1].strip()
            city = address.split(",")[-2].strip()
            if zipcode.isalpha():
                item_loader.add_value("city", zipcode)
            else:
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("city", city)

        item_loader.add_xpath("room_count","substring-after(//li[@class='bedrooms']/text(),':')")
        item_loader.add_xpath("bathroom_count","substring-after(//li[@class='bathrooms']/text(),':')")
     
        rent = response.xpath("//div[@class='price']/h4/text()").get()
        if rent:
            rent = rent.split("£")[-1].split("p")[0].replace(",","").strip()
            item_loader.add_value("rent", str(int(rent)*4))
            item_loader.add_value("currency", "GBP")
     
        description = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        images = [x for x in response.xpath("//div[@id='slider']/ul[@class='slides']/li/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
 
        item_loader.add_value("landlord_name", "LDG Estate & Letting Agents")
        item_loader.add_value("landlord_phone", "+44 (0)2075801010")
        item_loader.add_value("landlord_email", "hello@ldg.co.uk")
        lat_lng = response.xpath("//script[contains(.,'new google.maps.LatLng(')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split(" new google.maps.LatLng(")[-1].split(",")[0])
            item_loader.add_value("longitude", lat_lng.split(" new google.maps.LatLng(")[-1].split(",")[1].split(")")[0])
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None