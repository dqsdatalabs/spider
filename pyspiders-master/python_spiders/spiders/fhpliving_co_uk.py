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
import re
class MySpider(Spider):
    name = 'fhpliving_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://fhpliving.co.uk/to-let/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//a[contains(.,'More info')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            url = f"https://fhpliving.co.uk/to-let/page/{page}/"
            yield Request(url, callback=self.parse, meta={"page": page+1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Studio') or contains(.,'studio')]//text()").get()
        if property_type:
            item_loader.add_value("property_type", "studio")
        else:
            desc = "".join(response.xpath("//div[contains(@class,'description')]//text() | //div[@class='property-bullets']//li//text()").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            elif get_p_type_string(response.url):
                item_loader.add_value("property_type", get_p_type_string(response.url))
            else: return
        item_loader.add_value("external_source", "Fhpliving_Co_PySpider_united_kingdom")
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("=")[-1])
        externalcheck=item_loader.get_output_value("external_id")
        if externalcheck=="1219509":
            return 

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)
 
        address = response.xpath("//title//text()").get()
        if address:
            item_loader.add_value("address", address.split("-")[0])
        city=item_loader.get_output_value("title")
        if city:
            
            city=city.split("-")[0].split(",")[-1]
            if city==" " or city=="  ":
                city=item_loader.get_output_value("title") 
                city=city.split("-")[0].split(",")[-2]
                city=re.search("[A-Za-z].*",city)
                item_loader.add_value("city",city.group())

            else:
                item_loader.add_value("city",city)

        rent = "".join(response.xpath("//div[contains(@class,'property-info')]//h2/text()").getall())
        if rent:
            if "week" in rent.lower():
                rent = rent.split("£")[1].strip().split("per")[0].replace(",","")
                rent = int(rent)*4
            else:
                rent = rent.split("£")[1].strip().split("pcm")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'property-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Bed') or contains(.,'bed')]//text()").get()
        if room_count:
            room_count = room_count.lower().split("bed")[0].replace("double","").replace("spacious","").replace("stunning","").replace("luxury","").strip()
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except :
                    pass
        else:
            room_count = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Studio') or contains(.,'studio')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Bath')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split("bath")[0].strip()
            if " " in bathroom_count:
                bathroom_count = bathroom_count.split(" ")[-1]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                except :
                    pass
        
        images = [x for x in response.xpath("//div[contains(@id,'sliderBigReal')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Available') or contains(.,'AVAILABLE')]//text()").getall())
        if available_date:
            available_date = available_date.lower().split("available")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Parking') or contains(.,'parking') or contains(.,'Garage') or contains(.,'garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Furnished') or contains(.,'FURNISHED')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,' floor')]//text()").get()
        if floor:
            floor = floor.strip().split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@class,'property-bullets')]//li[contains(.,'EPC')]//text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "FHP LIVING")
        item_loader.add_value("landlord_phone", "0115 841 1155")
        item_loader.add_value("landlord_email", "info@fhpliving.co.uk")

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