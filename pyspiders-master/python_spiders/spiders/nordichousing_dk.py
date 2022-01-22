# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'nordichousing_dk'
    external_source = "Nordichousing_PySpider_denmark"
    execution_type = 'testing'
    country = 'denmark' 
    locale ='da'
    start_urls = ['https://www.nordichousing.dk/lej-en-bolig/?pageNumber=1']  # LEVEL 1

    
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-item ')]"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            status = item.xpath(".//div[@class='property-item__open-contact']/text()").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.nordichousing.dk/lej-en-bolig/?pageNumber={page}"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("=")[-1])
        
        property_type = response.xpath("//div/span[contains(.,'Ejendomstype')]/text()").get()
        if get_p_type_string(property_type):        
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        
        item_loader.add_value("external_source", self.external_source)

        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())
            
            zipcode = ""
            for i in title.split(" "):
                if len(i) == 4 and i.isdigit():
                    zipcode = i
                    break
            item_loader.add_value("zipcode", zipcode)
            if zipcode:
                city = title.split(zipcode)[1].strip().split(" ")[0]
                item_loader.add_value("city", city.replace(",",""))
        square_meters = response.xpath("substring-after(//div/span[contains(.,'Kvadratmeter')]/text(),':')").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
            
        room_count = response.xpath("substring-after(//div/span[contains(.,'stuer')]/text(),':')").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("substring-after(//div/span[contains(.,'badeværelser')]/text(),':')").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("substring-after(//div/span[contains(.,'husleje')]/text(),':')").get()
        if rent:
            rent = rent.strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "DKK")
        
        utilities = response.xpath("substring-after(//div/span[contains(.,'Estimeret')]/text(),':')").get()
        if utilities:
            utilities = utilities.strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)
        
        deposit = response.xpath("substring-after(//div/span[contains(.,'Depositum')]/text(),':')").get()
        if deposit:
            deposit = deposit.strip().split(" ")[0]
            if deposit.isdigit():
                depo = int(rent)*int(deposit)
                item_loader.add_value("deposit", depo)
        
        import dateparser
        available_date = response.xpath("substring-after(//div/span[contains(.,'Ledig')]/text(),':')").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        parking = response.xpath("substring-after(//div/span[contains(.,'Parkering')]/text(),':')").get()
        if parking and "ja" in parking.lower():
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("substring-after(//div/span[contains(.,'Altan')]/text(),':')").get()
        if balcony and "ja" in balcony.lower():
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("substring-after(//div/span[contains(.,'Terrasse')]/text(),':')").get()
        if terrace and "ja" in terrace.lower():
            item_loader.add_value("terrace", True)
        
        elevator = response.xpath("substring-after(//div/span[contains(.,'Elevator')]/text(),':')").get()
        if elevator and "ja" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        pets_allowed = response.xpath("substring-after(//div/span[contains(.,'Kæledyr')]/text(),':')").get()
        if pets_allowed and "ja" in pets_allowed.lower():
            item_loader.add_value("pets_allowed", True)
        
        furnished = response.xpath("substring-after(//div/span[contains(.,'Møblering')]/text(),':')").get()
        if furnished:
            if "umøbleret" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "møbleret" in furnished.lower():
                item_loader.add_value("furnished", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat":"')[1].split('"')[0]
            longitude = latitude_longitude.split('lng":"')[1].split('"')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        desc = "".join(response.xpath("//div[contains(@class,'desc')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor_plan_images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'gallery-floor')]//@style").getall()]
        item_loader.add_value("floor_plan_images", floor_plan_images)
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'gallery-photo')]//@style").getall()]
        item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Nordic Hosing")
        item_loader.add_xpath("landlord_phone", "//div[contains(.,'Kontakt ')]/span/text()")
        item_loader.add_value("landlord_email", "info@nordichosing.dk")
        
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and ("lejlighed" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("rækkehus" in p_type_string.lower() or "hus" in p_type_string.lower() or "attico" in p_type_string.lower()):
        return "house"
    else:
        return None