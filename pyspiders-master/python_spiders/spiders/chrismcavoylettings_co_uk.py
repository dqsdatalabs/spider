# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'chrismcavoylettings_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        yield Request("http://chrismcavoylettings.co.uk/properties.asp", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='content']/table"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'more')]/@href").get())
            is_available = item.xpath(".//img[@alt='To Let']").get()
            property_type = item.xpath(".//b[contains(.,'Bedroom')]/text()").get()
            if is_available and get_p_type_string(property_type): 
                yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("RecNum=")[-1])
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Chrismcavoylettings_Co_PySpider_united_kingdom")
        
        features = response.xpath("//td/b/text()").getall()
        item_loader.add_value("title", features[1])
        
        address = features[0]
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-3].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        
        rent = features[2]
        if rent:
            rent = rent.strip().split("\xa0")[0].replace(",","").replace("£","")
            item_loader.add_value("rent_string", rent+"£")
        
        room_count = response.xpath("//tr[contains(.,'Key')]/following-sibling::tr//tr/td[contains(.,'Bedroom')]//text()").get()
        if room_count:
            room_count = room_count.split("Bedroom")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", w2n.word_to_num(room_count))
        
        parking = response.xpath("//tr[contains(.,'Key')]/following-sibling::tr//tr/td[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//tr[contains(.,'Detailed')]/following-sibling::tr//div/span//text()[contains(.,'EPC')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(":")[1].strip())
        
        deposit = response.xpath("//tr[contains(.,'Detailed')]/following-sibling::tr//div/p//text()[contains(.,'Deposit') and not(contains(.,'Holding'))]").get()
        if deposit:
            deposit = deposit.split("week")[0].strip().split(" ")[-1]
            item_loader.add_value("deposit", int(rent)*int(deposit))
        
        description = " ".join(response.xpath("//tr[contains(.,'Detailed')]/following-sibling::tr//div/span//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//img/@src[contains(.,'images/thumb')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = response.xpath("//script[contains(.,'latitude:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude:')[1].split(',')[0].replace("'","").strip()
            longitude = latitude_longitude.split('longitude:')[1].split(',')[0].replace("'","").strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        item_loader.add_value("landlord_name", "Chris McAvoy Town & Country Lettings")
        item_loader.add_value("landlord_phone", "01827 718601")

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