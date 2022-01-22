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
    name = 'thepropertyplace_uk_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source = "Thepropertyplace_Uk_PySpider_united_kingdom"
    start_urls = ["https://www.thepropertyplace.uk.com/for-rent"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[contains(@data-testid,'item')]/a"):
            status = item.xpath(".//div[contains(@data-testid,'title')]/text()").get()
            if status and ("agreed" in status.lower() or "not" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        if response.url == "https://www.thepropertyplace.uk.com/for-rent":
            return
        f_text = " ".join(response.xpath("//h2[@class='font_2']//text()").getall())
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//p[@class='font_8']//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)

        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        item_loader.add_value("external_source", self.external_source)      
        title =" ".join(response.xpath("//div[1]/h2[1]//span/text()").extract())
        if not title:
            title =" ".join(response.xpath("//div[h2[contains(.,'£')]][1]//text()").extract())
        if title:
            item_loader.add_value("title",re.sub("\s{2,}", " ", title) )   
            address_1 = title.split("£")[0].strip()
            if address_1:
                address_1 = " ".join(address_1.split(",")[1:])
                if "Rooms" in address_1:
                    if "furnished," in title:
                        address_1 = title.split("furnished,")[1]
                else:
                    address_1= re.sub("\s{2,}", " ", address_1)
                item_loader.add_value("address",address_1) 
                zipcode = address_1.strip().split(" ")[-2]+" "+address_1.strip().split(" ")[-1]
                item_loader.add_value("zipcode", zipcode) 

        rent = response.xpath("substring-after(//div[h2[contains(.,'£')]][1]//text()[contains(.,'£')],'£')").extract_first()
        if rent:
            rent = rent.split(' ')[0].strip().replace(',', '').replace('\xa0', '').split("p")[0]
            item_loader.add_value("rent", str(int(float(rent))))
        item_loader.add_value("currency", 'GBP')
     
        available_date = response.xpath("//div/h2//span/text()[contains(.,'Available ') or contains(.,'AVAILABLE')]").extract_first() 
        if available_date:  
            date_parsed = dateparser.parse(available_date.lower().split("available")[1].strip(),date_formats=["%d-%m-%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        room_count = response.xpath("//ul/li/h2//span/text()[contains(.,'Bedrooms')]").extract_first() 
        if room_count:   
            room_count = room_count.lower().strip().split("bedrooms")[0].split("double")[0]
            item_loader.add_value("room_count", room_count)
  
        floor = response.xpath("//ul/li/h2//span/text()[contains(.,'Floor')]").extract_first()
        if floor:
            item_loader.add_value("floor", floor.split("Floor")[0].strip())
        balcony = response.xpath("//ul/li/h2//span/text()[contains(.,'Balcony')]").extract_first()
        if balcony:
            item_loader.add_value("balcony", True)
        furnished = response.xpath("//ul/li/h2//span/text()[contains(.,'Furnished') or contains(.,'furnished')]").extract_first()
        if furnished:
            if "unfurnished" in furnished.lower().replace(" ",""):
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        parking = response.xpath("//ul/li/h2//span/text()[contains(.,'Parking')]").extract_first()
        if parking:
            item_loader.add_value("parking",True)                
        desc = " ".join(response.xpath("//div/div[@class='txtNew'][last()]//span/text()[not(contains(.,'Hilton House, 71-73')) and not(contains(.,'Tel: 0161 222'))]").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        item_loader.add_value("landlord_name", "The Property Place")
        item_loader.add_value("landlord_phone", "0161 222 8550")
        item_loader.add_value("landlord_email", "info@thepropertyplace.uk.com")
        
        title = response.url.split("/")[-1].strip()

        script = response.xpath(f"//script[contains(.,'{title}') and contains(.,'pageJsonFileName')]").extract_first()
        if script:
            script = script.split('id="wix-viewer-model">')[1].split("</script>")[0].strip()
            data = json.loads(script)
            data = data["siteFeaturesConfigs"]["router"]["pages"]
            for i in data:
                image = f"https://static.wixstatic.com/media/{data[i]}~mv2_d_5184_3456_s_4_2.jpg"
                item_loader.add_value("images", image)
    
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None