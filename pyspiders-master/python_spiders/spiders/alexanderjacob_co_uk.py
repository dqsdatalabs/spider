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
    name = 'alexanderjacob_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ['http://www.alexanderjacob.co.uk/properties-to-let']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 12)
        
        seen = False
        for item in response.xpath("//div[@class='eapow-property-thumb-holder']"):
            follow_url = response.urljoin(item.xpath(".//@href").get())
            status = item.xpath(".//img[contains(@alt,'Let STC')]/@alt").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 12 or seen:
            url = f"http://www.alexanderjacob.co.uk/properties-to-let?start={page}"
            yield Request(url, callback=self.parse, meta={"page": page+12})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        desc = "".join(response.xpath("//div[contains(@class,'desc-wrapper')]/p/text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", "Alexanderjacob_Co_PySpider_united_kingdom")
 
        item_loader.add_value("external_id", response.url.split("property/")[1].split("-")[0])
        
        title = response.xpath("//title/text()").get() 
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = "".join(response.xpath("//h1/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        zipcode = response.xpath("//div[contains(@class,'address')]//address/text()").get()
        if zipcode:
            zipcode_ = zipcode.strip().split(" ")[-2] + " " + zipcode.strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode_)

        rent = response.xpath("//h1/small/text()").get()
        if rent:
            rent = rent.split("£")[1].strip().replace(",","").split(".")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//li[contains(.,'Security Deposit')]//text()").get()
        if deposit:
            deposit = deposit.split("Security Deposit")[1].replace("£","").replace("-","").replace(":","").strip().split(" ")[0]
            item_loader.add_value("deposit", int(float(deposit)))
        
        room_count = response.xpath("//img[contains(@src,'bed')]/following-sibling::strong[1]/text()[.!='0'] | //img[contains(@src,'rece')]/following-sibling::strong[1]/text()[.!='0']").get()
        item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//img[contains(@src,'bath')]/following-sibling::strong[1]/text()").get()
        item_loader.add_value("bathroom_count", bathroom_count)
        
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        furnished = response.xpath("//li[contains(.,'Furnished')][not(contains(.,'Un'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]//text()").get()
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        latitude_longitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat: "')[1].split('"')[0]
            longitude = latitude_longitude.split('lon: "')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        images = [x for x in response.xpath("//div[contains(@class,'thumbnail')]//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        name = response.xpath("//div[@class='span8']//b/text()").get()
        if name:
            item_loader.add_value("landlord_name", name)
        else:
            item_loader.add_value("landlord_name", "Alexander Jacob")
        
        phone = response.xpath("//b[contains(.,'T:')]/following-sibling::text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        else:
            item_loader.add_value("landlord_phone", "01777 566400")
        
        email = response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        else:
            item_loader.add_value("landlord_email", "info@alexanderjacob.co.uk")
        
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