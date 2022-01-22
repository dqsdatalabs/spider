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
    name = 'cloudhomes_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.cloudhomes.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&perpage=60"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='searchBtnRow']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.cloudhomes.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&bedrooms=&minprice=&maxprice=&perpage=60&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cloudhomes_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        f_text = " ".join(response.xpath("//section[@class='fullDetailWrapper']/article//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else:
            return
        
        item_loader.add_css("title", "title")
        
        address = response.xpath("//h3/text()").get()
        if address:
            item_loader.add_value("address", address)
            zipcode = address.split(",")[-1]
            city = address.split(zipcode)[0].strip().strip(",").strip(",")
            if "," in city:
                item_loader.add_value("city", city.split(",")[-1])
            else:
                item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode.strip())
        
        desc = " ".join(response.xpath("//article/h2[contains(.,'Full Details:')]//..//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            desc = desc.split("Full Details:")[1].split("Viewing & Disclaimer:")[0].strip()
            item_loader.add_value("description", desc)
        
        rent = "".join(response.xpath("//div[@class='fdPrice']/div/text()").getall())
        if rent:
            item_loader.add_value("currency", "GBP")
            rent = rent.split("£")[1].strip()
                
        rent_status = response.xpath("//li[contains(.,'p/w')]//text()").get()
        if rent_status:
            item_loader.add_value("rent", int(rent)*4)
        elif "pw" in desc.lower():
            item_loader.add_value("rent", int(rent)*4)
        else:
            item_loader.add_value("rent", rent)
            
        room_count = response.xpath("//div[@class='fdRooms']/span[contains(.,'bedroom')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//div[@class='fdRooms']/span[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        images = [x for x in response.xpath("//div[@class='gallery']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        furnished = response.xpath("//li[contains(.,' furnished') or contains(.,' FURNISHED') or contains(.,' Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available') or contains(.,'AVAILABLE ')]//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif "OF" in available_date:
                available_date = available_date.split("OF")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor.capitalize())
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'PARKING')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        washing_machine = response.xpath("//li[contains(.,'washing machine') or contains(.,'Washing machine') or contains(.,'WASHING MACHINE')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "CLOUD HOMES")
        item_loader.add_value("landlord_phone", "44 (0)1722 580263")
        item_loader.add_value("landlord_email", "info@cloudhomes.co.uk")
        
        #yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("double bedroom" in p_type_string.lower() or "single bedroom" in p_type_string.lower() or "en suite" in p_type_string.lower()):
        return "room"
    else:
        return None