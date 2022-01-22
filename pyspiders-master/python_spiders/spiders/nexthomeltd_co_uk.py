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
    name = 'nexthomeltd_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://www.onthemarket.com/agents/branch/next-home-leyton/properties/?search-type=to-rent&view=grid']  # LEVEL 1
    
    
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        seen = False
        for item in response.xpath("//div[@class='media']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 1 or seen:
            url = f"https://www.onthemarket.com/agents/branch/next-home-leyton/properties/?page={page}&search-type=to-rent&view=grid"
            yield Request(url, callback=self.parse, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if "?" in response.url:
            item_loader.add_value("external_link", response.url.split("?")[0])
        else:
            item_loader.add_value("external_link", response.url)
        prop_type = response.xpath("//div[@class='title title-details']/h1/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return

        item_loader.add_value("external_source", "Nexthomeltd_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("details/")[1].split("/")[0])

        item_loader.add_value("title", prop_type)

        address = response.xpath("//div[@class='title title-details']/p[@class='title-address']/text()").get()
        if address:
            city = address.split(",")[-2].strip()
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[@class='title title-details']/div[@class='price']/span/text()").get()
        if rent:
            price = rent.split("(")[1]
            if  "pw" in price:
                rent_pw = price.split("pw")[0].replace("£","").strip()
                item_loader.add_value("rent", int(rent_pw)*4)
            else:
                price = rent.split("pcm")[0].replace(",","")
                item_loader.add_value("rent", price)
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//script[contains(.,'Deposit:')]/text()").get()
        if deposit:
            deposit = deposit.split("Deposit:")[1].split('"')[0].strip()
            item_loader.add_value("deposit", deposit)
        else:
            deposit = response.xpath("//li[contains(.,'Deposit:')]//text()").get()
            if deposit:
                deposit = deposit.split("£")[1]
                item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[@item-prop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip()) 
            item_loader.add_value("description", desc)
        import dateparser
        available_date=response.xpath("//script[contains(.,'Availability')]/text()").get()
     
        if available_date:
            available_date=available_date.split("Availability date:")[-1].split(",")[0].replace('"',"").strip()
            print(available_date)
            if available_date:
                date_parsed = dateparser.parse(available_date, date_formats=["%m-%d-%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            
        room_count = response.xpath("//div[contains(@class,'property-bedrooms')]/span/text()").get()
        if room_count and room_count != '0':
            if "studio" in room_count.lower():                
                item_loader.add_value("room_count", "1")
            else:
                item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(., 'bedrooms')]/text()").re_first(r'(\d).*bedrooms')
            if room_count:
                item_loader.add_value("room_count", room_count)

        lat_lng = response.xpath("substring-after(substring-after(//script/text()[contains(.,'lon')],'contact-text'),'location')").get()
        if lat_lng:
            longitude = lat_lng.split("lon")[1].split(",")[0].replace('"','').replace(':','').strip()
            latitude = lat_lng.split("lat")[1].split("}")[0].replace('"','').replace(':','').strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        bathroom_count = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            bathroom_count = response.xpath("//div[@class='property-icon property-bathrooms']/span/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)           

        images = [x for x in response.xpath("//div[@class='hero']//picture//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//section[contains(@class,'letting-details')]/ul/li[contains(.,'Available from')]/text()").getall())

        if available_date:       
            available_date = available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        parking = response.xpath("//ul[contains(@class,'property-features')]//li[contains(.,'Garage') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//script[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        floor = response.xpath("//section[contains(@class,'property-features ')]/ul/li[contains(.,'Floor')]/text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].replace("one bedroom","").strip()
            item_loader.add_value("floor", floor.strip())
            
        washing_machine = response.xpath("//section[contains(@class,'property-features ')]/ul/li[contains(.,'Washing Machine') or contains(.,'washingmachine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        dishwasher = response.xpath("//section[contains(@class,'property-features ')]/ul/li[contains(.,'dishwasher') or contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_name", "NEXT HOME")
        item_loader.add_value("landlord_phone", "020 3641 9608")
        item_loader.add_value("landlord_email", "lettings@nexthomeltd.co.uk")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower() or "semi detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None