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
from word2number import w2n

class MySpider(Spider):
    name = 'purpleletting_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Purpleletting_PySpider_united_kingdom"
    
    def start_requests(self):

        start_urls = [
            {
                "type" : "1",
                "property_type" : "residental"
            },
            {
                "type" : "",
                "property_type" : "student_apartment"
            },
        ]
        for url in start_urls:

            formdata = {
                "minprice": "",
                "maxprice": "",
                "minbeds": "",
                "residential": url.get("type"),
                "minbeds": "",
                "minprice": "",
                "maxprice": "",
                "search": "",
            }

            yield FormRequest(
                url="https://www.purpleletting.com/search.vbhtml",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': url.get('property_type')}
            )

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='property-thumb-info']"):
            status = item.xpath("./div/a/div/span/@style").get()
            if status and "underoffer" in status:
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            if response.meta.get("property_type") == "residental":
                p_type_control = item.xpath("./div[contains(@class,'amenities')]/ul[@class='pull-left']/li/text()").get()
                if p_type_control and ("apartment" in p_type_control.lower() or "flat" in p_type_control.lower() or "maisonette" in p_type_control.lower()):
                    property_type = "apartment"
                elif p_type_control and "house" in p_type_control.lower():
                    property_type = "house"
                elif p_type_control and "studio" in p_type_control.lower():
                    property_type = "studio"
                else:
                    property_type = None
            else:
                property_type = response.meta.get("property_type")
            
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': property_type})
            seen = True
        
        if page == 2 or seen:
            formdata = {
                "minprice": "",
                "maxprice": "",
                "minbeds": "",
                "PropPerPage": "12",
                "order": "beds",
                "radius": "0",
                "letagreedorstc": "true",
                "grid": "",
                "search": "yes",
                "residential": "",
                "links": str(page),
            }

            yield FormRequest(
                url="https://www.purpleletting.com/search.vbhtml",
                callback=self.parse,
                formdata=formdata,
                meta={'property_type': response.meta.get('property_type'), "page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.meta.get('property_type')
        if property_type:
            item_loader.add_value("property_type", property_type)
        else:
            return
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
            address = title.strip().split(" ")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)

            if "terrace" in title.lower():
                item_loader.add_value("terrace", True)

        rent = response.xpath("//span[@class='fullprice2']/text()").get()
        if rent:
            price = str(int(rent.split("£")[1].replace(",",""))*4)
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//p[@class='photos-pad']/text()[contains(.,'Bedroom')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        
        bathroom_count = response.xpath("//p[@class='photos-pad']/text()[contains(.,'Bathroom')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip().split(" ")[0])
        
        available_date = response.xpath("//p[@class='photos-pad']/text()[contains(.,'Available')]").get()
        if available_date:
            available_date = available_date.split("Available")[1].strip()
            if "NOW" in available_date:
                available_date = datetime.now()
                item_loader.add_value("available_date", available_date.strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(
                        available_date, date_formats=["%d/%m/%Y"]
                    )
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        desc = "".join(response.xpath("//p[@class='lead']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        external_id = response.xpath("//p[contains(.,'Reference')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
        
        deposit = response.xpath("//p[@class='chg-orange']/../p/text()[contains(.,'Tenancy Deposit')]").get()
        if deposit:
            deposit = deposit.split("Rent")[0].split("to")[1]
            if "Months" in deposit:
                deposit = deposit.split("Months")[0].strip()
                item_loader.add_value("deposit", str(w2n.word_to_num(deposit)*int(price)))
        
        floor_plan_images = response.xpath("//div[@class='modal-body']/div[@class='fotorama']/img/@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", response.urljoin(floor_plan_images))
        
        energy_label = response.xpath("//div[@class='modal-content'][contains(.,'EPC')]//img/@src").get()
        if energy_label:
            energy_label = energy_label.split("epc1=")[1].split("&")[0]
            if energy_label != "0":
                item_loader.add_value("energy_label", energy_label_calculate(energy_label))
        
        images = [ x for x in response.xpath("//span[contains(@class,'propertystatus')]/parent::div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        lat_lng = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split("LatLng(")[1].split(",")[0]
            longitude = lat_lng.split("LatLng(")[1].split(",")[1].split(")")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        furnished = response.xpath("//ul[@class='key-features']/li[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//ul[@class='key-features']/li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
            
        parking = response.xpath("//ul[@class='key-features']/li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace = response.xpath("//ul[@class='key-features']/li[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        dishwasher = response.xpath("//ul[@class='key-features']/li[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//ul[@class='key-features']/li[contains(.,'Washing Machine')]/text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "Purple Letting")
        item_loader.add_value("landlord_phone", "01752 600 014")
        item_loader.add_value("landlord_email", "office@purpleletting.com")
        
        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label