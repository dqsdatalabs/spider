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
import re

class MySpider(Spider):
    name = 'expathousing_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.expathousing.com/nl/?search-listings=true"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='listing-featured-image']"):
            status = item.xpath("./preceding-sibling::h6/span/text()").get()
            if status and ("verhuurd" in status.lower() or "onder" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.expathousing.com/nl/page/{page}/?search-listings=true"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Expathousing_PySpider_netherlands")
        item_loader.add_value("external_link", response.url)

        f_text = "".join(response.xpath("//div[@id='listing-content']/p//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return

        street = response.xpath("//h1[@id='listing-title']//text()").get()
        item_loader.add_value("title", street)
        address = response.xpath("//p[contains(@class,'location')]//text()").get()
        if address or street:
            item_loader.add_value("address", street+" "+address)
        
        if address:
            zipcode = address.split(",")[0]
            city = address.split(",")[1].strip()
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//h4[contains(@class,'d-inline-block')]/span[contains(@class,'price')]//text()").get()
        if rent:
            item_loader.add_value("rent", rent.replace(".","").replace("€",""))
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//li/span[contains(.,'Slaapkamer')]/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//li/span[contains(.,'Gebied') or contains(.,'Oppervlakte')]/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters)
        
        desc = " ".join(response.xpath("//div[@id='listing-content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[@class='slides']//img/@src[not(contains(.,'data:image'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        available_date = response.xpath("//li/span[contains(.,'Beschikbaar')]/following-sibling::span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//div[@class='info-inner']//li[contains(.,'Opleverniveau')]/text()").get()
        if furnished:
            if "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
            elif "Ongemeubileerd" in furnished:
                item_loader.add_value("furnished", False)
        
        balcony = response.xpath("//div[@class='info-inner']//li[contains(.,'Balkon') or contains(.,'balkon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        deposit = response.xpath("//div[@id='listing-content']//text()[contains(.,'storting') or contains(.,'Borg') or contains(.,'borg')]").get()
        if deposit:
            rent = rent.replace(".","").replace("€","")
            deposit = deposit.lower().replace("maanden","").replace("borg","").replace("storting","").replace("x","").replace("-","").strip()
            if "twee" in deposit:
                deposit = int(rent)*2
            else:
                deposit = deposit.split(" ")[0]
                if deposit.isdigit():
                    deposit = int(deposit)*int(rent)
                else: deposit = ""
            if deposit:
                item_loader.add_value("deposit", int(deposit))
        
        utilities = response.xpath(
            "//div[@id='listing-content']//text()[contains(.,'Tv/internet') or contains(.,'internet/tv')][not(contains(.,'GWE'))]").get()
        utility = response.xpath(
            "//div[@id='listing-content']//text()[contains(.,'G/W/E') or contains(.,'GWE')][not(contains(.,'excl')) and not(contains(.,'Excl'))]").get()
        if utility:
            if "€" in utility:
                utility = utility.split("€")[1].strip().split(" ")[0]
            elif "EUR" in utility:
                utility = utility.split("EUR")[0].strip().split(" ")[-1]
            
            if utility.isdigit():
                item_loader.add_value("utilities", utility)
        elif utilities:
            utilities = utilities.split("€")[1].strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)
        
        elevator = response.xpath("//div[@id='listing-content']//text()[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        terrace = response.xpath("//div[@id='listing-content']//text()[contains(.,'terras')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//div[@id='listing-content']//text()[contains(.,'parkeer') or contains(.,'Parkeer')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        pets_allowed = response.xpath("//div[@id='listing-content']//text()[contains(.,'Huisdieren toegestaan')]").get()
        no_pets_allowed = response.xpath("//div[@id='listing-content']//text()[contains(.,'Geen huisdieren') or contains(.,'niet toegestaan')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)
        elif no_pets_allowed:
            item_loader.add_value("pets_allowed", False)
            
        
        item_loader.add_value("landlord_name", "EXPAT HOUSING")
        item_loader.add_value("landlord_phone", "31 (0)20-6622366")
        item_loader.add_value("landlord_email", "amsterdam@expathousing.com")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None