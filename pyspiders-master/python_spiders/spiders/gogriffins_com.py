# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser

class MySpider(Spider):
    name = 'gogriffins_com'
    execution_type = 'testing'
    country='united_kingdom'
    locale='en'
    external_source = "Gogriffins_PySpider_united_kingdom"
    start_urls = ['https://www.gogriffins.com/lettings']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
      
        for item in response.xpath("//div[@class='propertys_list ']"):
            follow_url = response.urljoin(item.xpath(".//a[h3]/@href").get())
            f_text = "".join(item.xpath(".//a//h3/text()").getall())
            if get_p_type_string(f_text):
                property_type = get_p_type_string(f_text)
            else:
                continue

            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        letting_status = "".join(response.xpath("//li[span[contains(.,'Letting')]]/text()").getall())
        if letting_status:
            letting_status = letting_status.strip()
            if "short" in letting_status.lower() :
                return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title/text()")


        item_loader.add_xpath("external_id", "normalize-space(//li[span[.='Reference number']]/text())")
        item_loader.add_xpath("room_count", "//li[i[contains(@class,'icon gnbicon-bedroom')]]/span[1]/text()")
        if response.meta.get("property_type") == "studio":
            item_loader.add_value("room_count", "1")

        item_loader.add_xpath("bathroom_count", "//li[i[contains(@class,'icon gnbicon-bathroom')]]/span[1]/text()")
        available_date = response.xpath("//li[span[contains(.,'Available from')]]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
       
        rent = response.xpath("//div[@id='price_value']/text()[contains(.,'pcm')]").get()
        if rent:
            item_loader.add_value("rent_string", rent.split("pcm")[0].split("/")[-1].split(".")[0])       

        address = "".join(response.xpath("//div[@class='property-detail-location']//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
      
        floor = response.xpath("//li//text()[contains(.,'Floor')]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[-1].strip())

        square = response.xpath("//li//text()[contains(.,'quare feet :')]").extract_first()
        if square:       
            square_title=square.split(":")[-1].strip()
            sqm = str(int(float(square_title) * 0.09290304))
            item_loader.add_value("square_meters", sqm)

        furnished = "".join(response.xpath("//li[span[contains(.,'Furnishing')]]/text()").getall())
        if furnished:
            if "unfurnished" in furnished.lower() :
                item_loader.add_value("furnished",False)
            else:
                item_loader.add_value("furnished",True)
        parking = "".join(response.xpath("//li[span[contains(.,'Parking')]]/text()").getall())
        if parking:
            if "yes" in furnished.lower() :
                item_loader.add_value("parking",True)

        desc = "".join(response.xpath("//div[@id='full_notice_description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        

        images = [x for x in response.xpath("//div[@id='slider']//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Griffins")
        item_loader.add_value("landlord_phone", "020 3837 0111")
        item_loader.add_value("landlord_email", "info@gogriffins.com")


        yield item_loader.load_item()

        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "duplex" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "house"
    else:
        return None