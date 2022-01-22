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
    name = 'houseshaw_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.houseshaw.co.uk/to-let/department/residential-lettings/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='thumbnail']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath(".//div[contains(@class,'flag')]/text()").get()
            if (status and "Available" in status) or not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.houseshaw.co.uk/to-let/department/residential-lettings/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        prop_type = response.xpath("//span[contains(.,'Type')]/following-sibling::text() | //div[@class='name']/text()").get()
        if prop_type and get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else:
            prop_type = "".join(response.xpath("//h1/text() | //div[@class='description']//text()").getall())
            if prop_type and get_p_type_string(prop_type):
                item_loader.add_value("property_type", get_p_type_string(prop_type))
            else: return
        
        item_loader.add_value("external_source", "Houseshaw_Co_PySpider_united_kingdom")
        
        item_loader.add_css("title", "h1")
        item_loader.add_css("address", "h1")
        
        city = response.xpath("//h1/text()").get()
        if city:
            if "," in city:
                item_loader.add_value("city", city.split(",")[-1])
            elif " – " in city:
                item_loader.add_value("city",city.split(" – ")[-1].strip())
            elif " –" not in city: item_loader.add_value("city", city)
                
        rent = "".join(response.xpath("//div[@class='price']/text()").getall())
        if rent:
            if "pw" in rent:
                rent = int(float(rent.split("£")[1].split(" ")[0].replace(",",".")))*4
            else:
                rent = rent.split("£")[1].split(" ")[0].replace(",","")
            if int(float(rent)) != 0: item_loader.add_value("rent", int(float(rent)))
            else:
                if response.xpath("//div[@class='rent']/text()").get():
                    rent = response.xpath("//div[@class='rent']/text()").get().split("£")[1].split(" ")[0]
                    if rent !="0": item_loader.add_value("rent", rent.replace(",",""))
        item_loader.add_value("currency", "GBP")
        
        description = " ".join(response.xpath("//div[@class='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        if "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        elif get_p_type_string(prop_type) == "room":
            item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//li[contains(.,'Bedroom')]/span/following-sibling::text()").get()
            if room_count: item_loader.add_value("room_count", room_count.strip())
            
        depo = response.xpath("//li[contains(.,'Deposit')]/span/following-sibling::text()").get()
        deposit = "".join(response.xpath("//h3[contains(.,'Tenancy')]/../text()[contains(.,'deposit')]").getall())
        if depo:
            depo = depo.split("£")[1].strip()
            item_loader.add_value("deposit", depo)
        elif deposit:
            deposit = deposit.split("week")[0].strip()
            if deposit.isdigit():
                deposit = int(deposit)*int((str(rent).replace(",","")))
                if int(float(deposit)) != 0: item_loader.add_value("deposit", int(deposit))        
        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/span/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        pet_allowed = "".join(response.xpath("//h3[contains(.,'Tenancy')]/../text()[contains(.,'Pets')]").getall())
        if "yes" in pet_allowed.lower():
            item_loader.add_value("pets_allowed", True)
        
        import dateparser
        available_date = response.xpath("//div[@class='available-date']/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[1].strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif response.xpath("//li[contains(.,'Available:')]/span/following-sibling::text()").get():
            available_date = response.xpath("//li[contains(.,'Available:')]/span/following-sibling::text()").get()
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]/span/following-sibling::text()").get()
        if furnished and "un" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        images = [x for x in response.xpath("//div[@class='image']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        washing_machine = response.xpath("//li[contains(.,'Washing')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        item_loader.add_value("landlord_name", "")
        item_loader.add_value("landlord_phone", "01494715619")
        
        
        status = response.xpath("//div[@class='availability']/text() | //li[contains(.,'Availability')]/span/following-sibling::text()").get()
        if status and "unavailable" in status.lower():
            return
        
        yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None