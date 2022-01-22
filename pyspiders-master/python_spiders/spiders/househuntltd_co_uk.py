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
    name = 'househuntltd_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Househuntltd_Co_PySpider_united_kingdom'
    start_urls = ['https://www.househuntltd.co.uk/properties?filter_keyword=&filter_cat=1&filter_beds=&filter_baths=&filter_price_low=min&filter_price_high=max&commit=&option=com_iproperty&view=allproperties&ipquicksearch=1']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='grid']"):
            prop_type = item.xpath(".//span[@class='title']//text()").get()
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            if get_p_type_string(prop_type):
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(prop_type)})
            else:
                yield Request(follow_url, callback=self.populate_item)
                
        last_page = response.xpath("substring-after(//span[@class='page-number']/text(),' of ')").get()
        if last_page:
            for i in range(1,int(last_page)):
                url = f"https://www.househuntltd.co.uk/properties?filter_cat=1&filter_price_low=min&filter_price_high=max&ipquicksearch=1&start={i*20}"
                yield Request(url, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        prop_type = response.meta.get('property_type')

        desc = "".join(response.xpath("//div[@class='description']//p//text()").getall())
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        elif get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return

        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//div[contains(@class,'code')]/text()").get()
        if external_id:
            external_id = external_id.replace(":","").strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//div[contains(@class,'mainheader')]//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'mainheader')]//h1//text()").get()
        if address:
            address = address.replace(":","").strip()
            if "," in address:
                zipcode = address.split(",")[-1].replace("Birmingham","").replace("Birimngham","").strip()
                if "-" in zipcode:
                    zipcode = zipcode.split("-")[0].strip()
            else:
                zipcode1 = address.strip().split(" ")[-1]
                zipcode2 = address.strip().split(" ")[-2]
                zipcode = zipcode2 + " " + zipcode1
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        city = response.xpath("//div[contains(@class,'city')]/text()").get()
        if city:
            city = city.replace(":","").strip()
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//div[contains(@class,'mainheader')]//span[contains(@class,'newprice')]//text()").get()
        if rent:
            if "week" in rent.lower():
                rent = rent.split("/")[0].replace("£","").strip()
                rent = int(rent)*4
            else:
                rent = rent.split("/")[0].replace("£","").strip()
            item_loader.add_value("rent", rent)
        else:
            rent = response.xpath("//div[contains(@class,'mainheader')]//h2//text()").get()
            if rent:
                if "week" in rent.lower():
                    rent = rent.split("/")[0].replace("£","").strip()
                    rent = int(rent)*4
                else:
                    rent = rent.split("/")[0].replace("£","").strip()
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'description')]//p//text()").getall()[:8])
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'bed')]/text()").get()
        if room_count:
            room_count = room_count.replace(":","").strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//div[contains(@class,'bath')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.replace(":","").strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'image-details')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//b[contains(.,'Available Date')]//following-sibling::span//text()").get()
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
            
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//li[contains(.,'Washer')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "House Hunt Ltd")

        item_loader.add_value("landlord_email", "lettings@househuntltd.co.uk")
        
        landlord_phone = response.xpath("//div[contains(@class,'phone')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None