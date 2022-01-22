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
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'vivirwonen_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source = "Vivirwonen_PySpider_netherlands"
    start_urls = ["https://www.pararius.nl/makelaars/gorinchem/vivir-wonen/huurwoningen"] #LEVEL-1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='listing-search-item__depiction']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
          
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//dt[contains(.,'Type woning')]//following-sibling::dd[1]//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        
        item_loader.add_xpath("title","//title/text()")
        
        address = response.xpath("//div/h1/parent::div/div[contains(@class,'location')]/text()").get()
        if address:
            item_loader.add_value("address", address)
            address2 = address.split(" ")
            zipcode = address2[0]+address2[1]
            city = address.split("(")[1].split(")")[0]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//dt[contains(.,'Huurprij')]/following-sibling::dd[1]//text()[normalize-space()]").get()
        if rent:
            if "€" in rent:
                price = rent.split("per")[0].split("€")[1].replace(" ","").replace(".","")
                item_loader.add_value("rent", price.strip())
            item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//dt[contains(.,'Woonoppervlakte')]/following-sibling::dd//text()[normalize-space()]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        room_count = response.xpath("//dt[contains(.,'slaapkamers')]/following-sibling::dd//text()[normalize-space()]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//dt[contains(.,'badkamers')]/following-sibling::dd//text()[normalize-space()]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        desc = " ".join(response.xpath("//div/h2[contains(.,'Beschrijving')]/parent::div//text()[normalize-space()]").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "verdieping" in desc.lower():
            floor = desc.lower().split("verdieping")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        images = [x for x in response.xpath("//img[@class='picture__image']/@src[contains(.,'http')]").getall()]
        if images:
            item_loader.add_value("images", images)        
        
        latitude = response.xpath("//div/@data-detail-map-latitude").get()
        longitude = response.xpath("//div/@data-detail-map-longitude").get()
        if latitude or longitude:
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        available_date = response.xpath("//dt[contains(@class,'features__term')][contains(.,'Beschikbaar')]/following-sibling::dd[1]//text()[normalize-space()]").get()
        if available_date:
            available_date = available_date.split("Per")[-1].strip()
            if "direct" not in available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//dt[contains(.,'Bijzonderheden')]/following-sibling::dd//text()[normalize-space()]").get()
        if furnished:
            if "Gestoffeerd" in furnished or "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//dt[contains(.,'Voorzieningen')]/following-sibling::dd//text()[normalize-space()]").get()
        if terrace:
            if "terras" in terrace:
                item_loader.add_value("terrace", True)
            if "lift" in terrace.lower():
                item_loader.add_value("elevator", True)
        
        balcony = response.xpath("//dt[contains(.,'Balkon')]/following-sibling::dd//text()[normalize-space()]").get()
        if balcony:
            if "Niet aanwezig" in balcony:
                item_loader.add_value("balcony", False)
            elif "Aanwezig" in balcony:
                item_loader.add_value("balcony", True)
        
        garage = response.xpath("//section/h2[contains(.,'Garage')]/parent::section//dt[contains(.,'Aanwezig')]/following-sibling::dd//text()[normalize-space()]").get()
        parking = response.xpath("//section/h2[contains(.,'Parkeergelegenheid')]/parent::section//dt[contains(.,'parkeergelegenheid')]/following-sibling::dd//text()[normalize-space()]").get()
        if parking and "Aanwezig" in parking:
                item_loader.add_value("parking", True)
        elif garage and "Ja" in garage:
                item_loader.add_value("parking", True)

        
        item_loader.add_value("landlord_name", "VIVIR WONEN")
        phone = "".join(response.xpath("//div[@class='agent-summary__links']//a[contains(@href,'tel:')]/text()").getall())
        if phone:
            phone = phone.split("+")[1].strip()
            item_loader.add_value("landlord_phone", phone)

        
        status = response.xpath("//span[contains(@class,'status')][contains(.,'Verhuurd')]/text()").get()
        if not status:
            yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("huis" in p_type_string.lower() or "house" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None