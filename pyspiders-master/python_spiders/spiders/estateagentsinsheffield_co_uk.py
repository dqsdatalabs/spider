# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek 

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime
import dateparser
from word2number import w2n
class MySpider(Spider):
    name = 'estateagentsinsheffield_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["https://www.estateagentsinsheffield.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1&officeids=4"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[@class='fdLink']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://www.estateagentsinsheffield.co.uk/Search?listingType=6&statusids=1&obc=Price&obd=Descending&areainformation=&radius=&minprice=&maxprice=&bedrooms=&cipea=1&officeids=4&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_id", response.url.split("?")[0].split("/")[-1])
        f_text = " ".join(response.xpath("//div[contains(@class,'fdDescription')]//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: 
            return
        item_loader.add_value("external_source", "Estateagentsinsheffield_Co_PySpider_united_kingdom")
        
        item_loader.add_xpath("title", "//div[@class='row detailsItem']//h2/text()")
        address = response.xpath("//div[@class='row detailsItem']//h2/text()").get()
        if address:
            item_loader.add_value("address", address.replace("- ",""))
            zipcode = ""
            city = ""
            if len(address.split(",")) >2:
                zipcode = address.split(",")[-1]
                city = address.split(",")[-2]
            elif len(address.split(",")) > 1:
                zipcode = address.split(",")[-1]
            if city:
                item_loader.add_value("city",city.strip())
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())

        rent = "".join(response.xpath("//div[@class='row detailsItem']//h3/div/text()[normalize-space()]").getall())
        if rent:
            if "pw" in rent.lower():
                rent = rent.lower().split('£')[-1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:
                rent = rent.lower().split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", rent)     
        
        item_loader.add_value("currency", "GBP")
    
        available_date =response.xpath("//div[@class='fdFeatures']//ul/li//text()[contains(.,'Available') or contains(.,'AVAILABLE')]").get()
        if available_date:
            if "now" in available_date:
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                available_date = available_date.lower().split("available")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        room_count = response.xpath("//div[@class='fdFeatures']//ul/li//text()[contains(.,'BEDROOM')]").get()
        if "studio" in prop_type or "room" in prop_type:
            item_loader.add_value("room_count", "1")
        # elif room_count:        
        #     room_count = room_count.split("BEDROOM")[0].strip().split(" ")[-1]
        #     room = w2n.word_to_num(room_count)
        #     if room: 
        #         item_loader.add_value("room_count",room)
        # else:
        #     room_count = response.xpath("//div[@class='fdFeatures']//ul/li//text()[contains(.,'Bedroom')]").get()
        #     if room_count and "double bedroom" in room_count.lower():
        #         item_loader.add_value("room_count", "1")
            
        parking = response.xpath("//div[@class='fdFeatures']//ul/li//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//div[@class='fdFeatures']//ul/li//text()[contains(.,'Furnished') or contains(.,'furnished') ]").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
       
        desc = " ".join(response.xpath("//div[@class='row fdDescription']//text()[.!='Description']").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        deposit = "".join(response.xpath("//div[contains(@class,'Description')]//text()[contains(.,'deposit')] | //text()[contains(.,'Deposit £')]").getall())
        if deposit:
            deposit = deposit.split("£")[-1].strip().split(" ")[0]
            item_loader.add_value("deposit", int(float(deposit)))
            
        images = [ x for x in response.xpath("//div[@id='property-photos-device1']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name","Chadwicks Estate Agents")
        item_loader.add_value("landlord_phone","0114 2994444")
        item_loader.add_value("landlord_email","admin@chadwicksestateagents.co.uk")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("flat" in p_type_string.lower() or "terrace" in p_type_string.lower() or "apartment" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    elif p_type_string and "suite" in p_type_string.lower():
        return "room"
    else:
        return None